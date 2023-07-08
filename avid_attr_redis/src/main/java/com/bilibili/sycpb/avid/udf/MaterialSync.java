package com.bilibili.sycpb.avid.udf;

import com.bilibili.sycpb.avid.redis.RedisByteSet;
import com.bilibili.sycpb.avid.redis.RedisOutputByteFormat;
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


public class MaterialSync {
    private static final Logger logger = LoggerFactory.getLogger(MaterialSync.class);
    public static final String LOG_DATE = "log_date";
    public static final String INPUT_TABLE = "input.table";

    private static Options options = new Options();


    @SuppressWarnings("static-access")
    private static void setupOptions() {
        // create Options object
        //可以进行分析的hive表路径
        options.addOption(OptionBuilder.withLongOpt(INPUT_TABLE).withDescription("input table")
                .hasArg().withArgName("[path1]").create());
        options.addOption(OptionBuilder.withLongOpt(LOG_DATE).withDescription("log_date")
                .hasArg().withArgName("log_date").create());

    }

    static {
        setupOptions();
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLineParser cmdParser = new GnuParser();
        String log_date = null;
        String inputTable = null;
        try {
            CommandLine line = cmdParser.parse(options, otherArgs);
            if (line.hasOption(LOG_DATE)) {
                log_date = line.getOptionValue(LOG_DATE);
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
                .appName("MaterialSync")
                .config(new SparkConf())
                .enableHiveSupport()
                .getOrCreate();
        String SQL="select case when key_type='unit_type' then concat('unit_type_',key_id) when key_type='query_type' then concat('query_type_',key_id) when key_type='acc_type' then concat('acc_type_',key_id)  when key_type='avid_type'  then concat('avid_type_',key_id) end as redis_key, concat('{\"data\":[',concat_ws(',',collect_set(`data1`)),'],','\"version\":\"', max(version),'\"}') as redis_value from (select key_id,key_type,concat('{\"fea_id\":\"',fea_id,'\",','\"fea_value\":\"',fea_value,'\"}') as `data1`,`log_date` as version from  " + inputTable + " where log_date=" + log_date + " and length(fea_id)>0 and length(fea_value)>0 and key_type in ('unit_type','query_type','acc_type','avid_type') ) t group by key_id,key_type";

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


    private static void printUsage(String errorMessage) {
        System.err.println("ERROR: " + errorMessage);
        printUsage();
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TagIndexExporter", options);
    }

}
