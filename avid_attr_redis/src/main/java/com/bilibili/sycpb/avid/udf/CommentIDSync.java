package com.bilibili.sycpb.avid.udf;

import com.bilibili.sycpb.avid.redis.RedisByteSet;
import com.bilibili.sycpb.avid.redis.RedisOutputByteFormat.RedisRecordWriter;
import com.bilibili.sycpb.avid.utils.Constants;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class CommentIDSync {
    private static final Logger logger = LoggerFactory.getLogger(CommentIDSync.class);
    public static final String LOG_DATE = "log_date";

    private static Options options = new Options();


    @SuppressWarnings("static-access")
    private static void setupOptions() {
        // create Options object
        //可以进行分析的hive表路径
        options.addOption(OptionBuilder.withLongOpt(LOG_DATE).withDescription("log_date").hasArg().withArgName("log_date").create());

    }

    static {
        setupOptions();
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLineParser cmdParser = new GnuParser();
        String log_date = null;
        try {
            CommandLine line = cmdParser.parse(options, otherArgs);
            if (line.hasOption(LOG_DATE)) {
                log_date = line.getOptionValue(LOG_DATE);
            } else {
                printUsage("Please specify 'dict_version'");
                return;
            }

        } catch (Exception e) {
            logger.error("Unexpected exception:" + e.getMessage(), e);
        }

        SparkSession spark = SparkSession.builder().appName("CommentIDSync").config(new SparkConf()).enableHiveSupport().getOrCreate();
        Dataset<Row> redisDF = spark.sql("select concat('reply_id_',reply_id) as redis_key,'{}' as redis_value from bili_sycpb.dwd_socl_blue_link_basic_info_a_hr where concat(log_date, log_hour) = " + log_date + " and reply_id is not null and reply_id > 1 and business_type in ('带货', '花火', '普通效果') group by reply_id");

        redisDF.show(10);
        Dataset<Row> persistDF = redisDF.persist(StorageLevel.MEMORY_AND_DISK());
        persistDF.repartition(120).foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                parseIterator(iterator);
            }
        });
        spark.stop();
    }

    private static void parseIterator(Iterator<Row> iterator) {
        JedisCluster jc = new JedisCluster(parseHosts(Constants.COMMENT_BASE));
        RedisRecordWriter redisRecordWriter = new RedisRecordWriter(jc, 1000);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String redisKey = row.getAs("redis_key").toString().trim();
            String redisValue = row.getAs("redis_value").toString().trim();
            RedisByteSet redisByteSet = new RedisByteSet(redisKey.getBytes(), redisValue.getBytes(), Constants.COMMENT_BASE_EXPIRE);
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
