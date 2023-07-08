package com.bilibili.sycpb.avid.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class DateCenter {
    private static final Log logger = LogFactory.getLog(DateCenter.class);
    public static final String LOGDATE = "log_date";
    public static final String FEAID = "fea_id";
    public static final String INTERVALDATE = "interval_date";
    private static Options options = new Options();

    public DateCenter() {
    }

    private static void setupOptions() {
        options.addOption(OptionBuilder.withLongOpt(LOGDATE).withDescription("log_date")
                .hasArg().withArgName("log_date").create());
        options.addOption(OptionBuilder.withLongOpt(FEAID).withDescription("fea_id")
                .hasArg().withArgName("fea_id").create());
        options.addOption(OptionBuilder.withLongOpt(INTERVALDATE).withDescription("interval_date")
                .hasArg().withArgName("interval_date").create());

    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        CommandLineParser cmdParser = new GnuParser();
        String log_date_end = null;
        String sparkSQLStr = "";

        try {
            CommandLine line = cmdParser.parse(options, otherArgs);
            if (!line.hasOption("log_date") || !line.hasOption("fea_id")) {
                printUsage("Please specify 'log_date fea_id'");
                return;
            }

            String[] splitFeaIdArray = line.getOptionValue("fea_id").split(",");
            String[] splitIntervalArray = line.getOptionValue("interval_date").split(",");
            log_date_end = line.getOptionValue("log_date");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            Date dateEnd = formatter.parse(log_date_end);

            for(int i = 0; i < splitFeaIdArray.length; ++i) {
                Calendar calendar = new GregorianCalendar();
                calendar.setTime(dateEnd);
                calendar.add(5, -Integer.parseInt(splitIntervalArray[i]));
                String log_date_start = formatter.format(calendar.getTime());
                if (StringUtils.isBlank(sparkSQLStr)) {
                    sparkSQLStr = sparkSQLStr + "( log_date between " + log_date_start + " and " + log_date_end + " and fea_id = " + splitFeaIdArray[i] + " )";
                } else {
                    sparkSQLStr = sparkSQLStr + " or ( log_date between " + log_date_start + " and " + log_date_end + " and fea_id = " + splitFeaIdArray[i] + " )";
                }
            }
        } catch (Exception var14) {
            logger.error("Unexpected exception:" + var14.getMessage(), var14);
        }

        SparkSession spark = SparkSession.builder().appName("DateCenter").config(new SparkConf()).enableHiveSupport().getOrCreate();
        Dataset<Row> dataset = spark.sql("select fea_id,mid,concat_ws('&_&',collect_set(fea)) as fea_array from bili_sycpb.offline_data_center_feature where fea is not null and length(fea) >5 and ( " + sparkSQLStr + " ) group by fea_id,mid");
        dataset.show(10);
        System.out.println("sparkSQLStr:" + sparkSQLStr);
        JavaRDD<Row> rowJavaRDD = dataset.toJavaRDD().mapPartitions((tup) -> {
            List<Row> rows = new LinkedList();

            while(tup.hasNext()) {
                Row next = (Row)tup.next();
                String mid = next.getAs("mid").toString().trim();
                String feaId = next.getAs("fea_id").toString().trim();
                String fea_array = next.getAs("fea_array").toString().trim();
                String[] split = fea_array.split("&_&");
                ArrayList<JSONObject> feaJsonList = new ArrayList();

                for(int i = 0; i < split.length; ++i) {
                    feaJsonList.add(JSONObject.parseObject(split[i]));
                }

                feaJsonList.sort(new Comparator<JSONObject>() {
                    public int compare(JSONObject o1, JSONObject o2) {
                        return Long.compare(Long.parseLong(o2.getString("ts")), Long.parseLong(o1.getString("ts")));
                    }
                });
                Row row2 = RowFactory.create(new Object[]{mid, feaJsonList.toString(), feaId});
                rows.add(row2);
            }

            return rows.iterator();
        });
        StructField[] fields = new StructField[]{DataTypes.createStructField("mid", DataTypes.StringType, false), DataTypes.createStructField("fea_array", DataTypes.StringType, false), DataTypes.createStructField("feaId", DataTypes.StringType, false)};
        StructType scheme = DataTypes.createStructType(fields);
        Dataset<Row> newDataFrame = spark.createDataFrame(rowJavaRDD, scheme);
        newDataFrame.createOrReplaceTempView("data_center_table");
        Dataset<Row> sql = spark.sql(" select mid,fea_array from data_center_table");
        sql.show(50);
        spark.sql("set hive.exec.dynamic.partition=true");
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
        spark.sql("set hive.exec.max.dynamic.partitions=500");
        spark.sql("set hive.exec.max.dynamic.partitions.pernode=500");
        spark.sql("insert overwrite table bili_sycpb.dws_ctnt_data_center_aggregate_d partition (log_date='" + log_date_end + "',fea_id) select mid,fea_array,feaId from data_center_table");
        spark.stop();
    }

    private static void printUsage(String errorMessage) {
        System.err.println("ERROR: " + errorMessage);
        printUsage();
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TagIndexExporter", options);
    }

    static {
        setupOptions();
    }
}
