package com.bilibili.sycpb.avid.udf;

import com.bilibili.sycpb.avid.redis.RedisByteSet;
import com.bilibili.sycpb.avid.redis.RedisOutputByteFormat;
import com.bilibili.sycpb.avid.utils.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class GameTagSync {
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("GameTagSync")
                .config(new SparkConf())
                .enableHiveSupport()
                .getOrCreate();
       // String SQL = "select concat('game_tag_', customer_id, product_id) as redis_key,concat('{\"game_tags\":\"',concat('[\\\\\"',replace(concat_ws(',', split(game_tag_id, '_')), ',',  '\\\\\",\\\\\"' ),'\\\\\"]'),'\",','\"game_avtags\":\"',concat('[\\\\\"',replace(concat_ws(',', split(game_avtag_id, '_')), ',', '\\\\\",\\\\\"' ),'\\\\\"]' ),'\"}') as redis_value from sycpb.game_tags_info_v1_a";
        String SQL = "select concat('game_tag_', customer_id, product_id) as redis_key,concat('{\"game_tags\":\"',COALESCE(game_tag_id,''),'\",','\"game_avtags\":\"',COALESCE(game_avtag_id,''),'\"}') as redis_value from sycpb.game_tags_info_v1_a";
        Dataset<Row> redisDF = spark.sql(SQL);
        System.out.println("spark_sql" + SQL.toString());
        redisDF.show(10);
        Dataset<Row> persistDF = redisDF.persist(StorageLevel.MEMORY_AND_DISK());
        persistDF.repartition(120).foreachPartition((ForeachPartitionFunction<Row>) itertator -> parseIterator(itertator));
        spark.stop();
    }
    private static void parseIterator(Iterator<Row> iterator) {
        JedisCluster jc = new JedisCluster(parseHosts(Constants.FEATURE_SYNC));
        RedisOutputByteFormat.RedisRecordWriter redisRecordWriter = new RedisOutputByteFormat.RedisRecordWriter(jc, 1000);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String redisKey = row.getAs("redis_key").toString().trim();
            String redisValue = row.getAs("redis_value").toString().trim();
            RedisByteSet redisByteSet = new RedisByteSet(redisKey.getBytes(), redisValue.getBytes(), Constants.GAME_TAG_TIME);
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

}
