package com.bilibili.sycpb.avid.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.*;
import java.io.Closeable;
import java.io.Serializable;
import java.util.*;

public class RedisOutputByteFormat implements Serializable {
    private static final Log LOG = LogFactory.getLog(RedisOutputByteFormat.class);
    public final static String REDIS_CLUSTER_KEY = "spark.redis.host";
    public final static String REDIS_FLUSH_BUFFER_NUM_KEY = "redis.flush.buffernum";


    public static class RedisRecordWriter implements Serializable {
        private static final Log LOG = LogFactory.getLog(RedisRecordWriter.class);
        private static final int maxErrorNum = 10000;
        public static final String REDIS_ERROR_COUNT = "REDIS_ERROR_COUNT";
        private JedisCluster client;
        private JedisPoolManager poolMgr;
        private LinkedList<RedisByteSet> opBuffer;
        private int errorCount = 0;
        private int maxBufferNum;

        public RedisRecordWriter(JedisCluster jc, int maxBufferNum) {
            this.client = jc;
            this.poolMgr = new JedisPoolManager(jc);
            this.maxBufferNum = maxBufferNum;
            this.opBuffer = new LinkedList<RedisByteSet>();
        }

        public void close() {
            // if has buffer left
            if (opBuffer.size() > 0) {
                // flush to redis
                flushCommits();
            }
            // close jedis cluster
            closeQuietly(client);
        }

        public void write(RedisByteSet operation) {
            // buffer
            opBuffer.add(operation);
            // if buffer size > N
            if (opBuffer.size() >= maxBufferNum) {
                // flush to redis
                flushCommits();
            }
        }

        protected void flushCommits() throws RuntimeException {
            Map<JedisPool, List<RedisByteSet>> maps = new HashMap<>();
            try {
                long tid = Thread.currentThread().getId();
                LOG.info(String.format("Thread# %x, %s flush %d rows", tid, this.toString(), opBuffer.size()));
                for (RedisByteSet v : opBuffer) {
                    String redisKey = v.getKey();
                    LOG.info(String.format("Thread# %x, %s flush ", tid,redisKey));
                    JedisPool pool = poolMgr.getJedisPool(redisKey);
                    if (pool == null) {
                        throw new RuntimeException("get JedisPool for key '" + redisKey + "' failed");
                    }
                    List<RedisByteSet> hl = maps.get(pool);
                    if (hl == null) {
                        hl = new LinkedList<>();
                        hl.add(v);
                        maps.put(pool, hl);
                    } else {
                        hl.add(v);
                    }
                }
                for (Map.Entry<JedisPool, List<RedisByteSet>> e : maps.entrySet()) {
                    JedisPool pool = e.getKey();
                    int ops = pipelineFlush(pool, e.getValue());
                    LOG.info(String.format("JedisPool ops %d", ops));
                }
            } catch (Exception e) {
                throw e;
            } finally {
                opBuffer.clear();
            }
        }

        private void closeQuietly(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    LOG.warn("close fail: " + closeable.toString(), ignored);
                }
            }
        }

        int parseOpsFromStats(String stats) {
            String[] statLines = stats.trim().split("\r\n");
            final String opsKey = "instantaneous_ops_per_sec";
            for (String l : statLines) {
                if (l.startsWith(opsKey)) {
                    return Integer.parseInt(l.substring(opsKey.length() + 1));
                }
            }
            return 0;
        }

        private int pipelineFlush(JedisPool pool, List<RedisByteSet> values) {
            Jedis jd = pool.getResource();
            Client cl = jd.getClient();
            LOG.debug(String.format("JedisPool %x %s:%d write %d keys",
                    pool.hashCode(), cl.getHost(),
                    cl.getPort(), values.size()));
            Pipeline pipeline = jd.pipelined();
            int ops = 0;
            try {
                for (RedisByteSet op: values) {
                    String k = op.getKey();
                    op.execute(pipeline);
                }
                Response<String> stats = pipeline.info("stats");

                pipeline.sync();

                ops = parseOpsFromStats(stats.get());
            } catch (Exception e) {
                LOG.warn("pipeline flush fail", e);
                errorCount += values.size();
                if (errorCount > maxErrorNum) {
                    throw new RuntimeException("too many pipeline flush error");
                }
            } finally {
                closeQuietly(pipeline);
                closeQuietly(jd);
            }
            return ops;
        }
    }

    public RedisRecordWriter getRecordWriter(String sHosts) {
        Set<HostAndPort> jedisClusterNodes = parseHosts(sHosts);
        //Jedis Cluster will attempt to discover cluster nodes automatically
        JedisCluster jc = new JedisCluster(jedisClusterNodes);
        Map<String, JedisPool> nodes = jc.getClusterNodes();
        LOG.info("init redis cluster node " + nodes.size());
        for (Map.Entry<String, JedisPool> e : nodes.entrySet()) {
            LOG.info("cluster node: " + e.getKey() + ", " + e.getValue().toString());
        }
        return new RedisRecordWriter(jc, 100);
    }

    private Set<HostAndPort> parseHosts(String sHosts) {
        String[] sHostPorts = sHosts.trim().split(",");
        if (sHostPorts.length == 0) {
            throw new RuntimeException("redis cluster hosts length = 0");
        }
        Set<HostAndPort> nodeSet = new HashSet<>();
        for (String sPair : sHostPorts) {
            String[] pair = sPair.trim().split(":");
            if (pair.length != 2) {
                throw new RuntimeException("invalid host pair string: " + sPair);
            }
            LOG.info("add new redis host " + pair[0] + ":" + Integer.parseInt(pair[1]));
            nodeSet.add(new HostAndPort(pair[0], Integer.parseInt(pair[1])));
        }
        return nodeSet;
    }
}

