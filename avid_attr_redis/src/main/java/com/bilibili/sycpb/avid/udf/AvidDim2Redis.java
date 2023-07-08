package com.bilibili.sycpb.avid.udf;

import com.bilibili.sycpb.avid.redis.RedisByteSet;
import com.bilibili.sycpb.avid.redis.RedisOutputByteFormat.RedisRecordWriter;
import com.bilibili.sycpb.avid.utils.Constants;
import com.bilibili.sycpb.flink.api.AvidAttributeProfile;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class AvidDim2Redis {
    private static final Logger logger = LoggerFactory.getLogger(AvidDim2Redis.class);
    public static final String DICT_VERSION = "dict_version";
    public static final String INPUT_TABLE = "input.table";

    private static Options options = new Options();


    @SuppressWarnings("static-access")
    private static void setupOptions() {
        // create Options object
        //可以进行分析的hive表路径
        options.addOption(OptionBuilder.withLongOpt(INPUT_TABLE).withDescription("input table")
                .hasArg().withArgName("[path1]").create());
        options.addOption(OptionBuilder.withLongOpt(DICT_VERSION).withDescription("dict_version")
                .hasArg().withArgName("dict_version").create());

    }

    static {
        setupOptions();
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLineParser cmdParser = new GnuParser();
        String dict_version = null;
        String inputTable = null;
        try {
            CommandLine line = cmdParser.parse(options, otherArgs);
            if (line.hasOption(DICT_VERSION)) {
                dict_version = line.getOptionValue(DICT_VERSION);
            } else {
                printUsage("Please specify 'dict_version'");
                return;
            }
            if (line.hasOption(INPUT_TABLE)) {
                inputTable = line.getOptionValue(INPUT_TABLE);
            } else {
                printUsage("Please specify 'input.table'");
                return;
            }
        } catch (Exception e) {
            logger.error("Unexpected exception:" + e.getMessage(), e);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("avidDim2Redis")
                .config(new SparkConf())
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> redisDF = spark.sql("select avid, mid, tid, sub_tid, tag_ids, video_duration, bussniess_avid from " + inputTable + " where dict_version=" + dict_version);
        redisDF.show(10);
        Dataset<Row> persistDF = redisDF.persist(StorageLevel.MEMORY_AND_DISK());
        persistDF.repartition(120).foreachPartition((ForeachPartitionFunction<Row>) itertator -> parseIterator(itertator));
        spark.stop();
    }

    private static void parseIterator(Iterator<Row> iterator) {
        JedisCluster jc = new JedisCluster(parseHosts(Constants.REDIS_ADDRESS));
        RedisRecordWriter redisRecordWriter = new RedisRecordWriter(jc, 1000);
        // 得到条数据封装为pb写redis
        while (iterator.hasNext()) {
            AvidAttributeProfile.AvidAttribute.Builder builder = AvidAttributeProfile.AvidAttribute.newBuilder();
            Row row = iterator.next();

            String redisKey = row.getAs("avid").toString().trim();
            long[] tag_ids = Arrays.stream(row.getAs("tag_ids").toString().trim().split(",", -1)).mapToLong(Long::parseLong).toArray();
            List<Long> longList = Arrays.stream(tag_ids).boxed().collect(Collectors.toList());

            builder.setMid(Long.parseLong(row.getAs("mid").toString()))
                    .setTid(Integer.parseInt(row.getAs("tid").toString()))
                    .setSubTid(Integer.parseInt(row.getAs("sub_tid").toString().trim()))
                    .addAllTags(longList)
                    .setVideoDuration(Integer.parseInt(row.getAs("video_duration").toString().trim()))
                    .setIsBussiness(Integer.parseInt(row.getAs("bussniess_avid").toString().trim()));


            //构建对象
            AvidAttributeProfile.AvidAttribute avidAttribute = builder.build();
            //     写入redis
            String key = "avid_" + redisKey;
            RedisByteSet redisByteSet = new RedisByteSet(key.getBytes(), avidAttribute.toByteArray(), Constants.REDIS_EXPIRE);
            redisRecordWriter.write(redisByteSet);
        }
        redisRecordWriter.close();
    }

    private static Set<HostAndPort> parseHosts(String sHosts) {
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
            nodeSet.add(new HostAndPort(pair[0], Integer.parseInt(pair[1])));
        }
        return nodeSet;
    }


    private static void printUsage(String errorMessage) {
        System.err.println("ERROR: " + errorMessage);
        printUsage();
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TagIndexExporter", options);
    }

}
