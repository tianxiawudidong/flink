package com.ifchange.flink.hive;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * data-app-tob 数据流监控
 * 1、流量监控
 * 请求量  m【5分钟】
 */
public class FlinkForTob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkForTob.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(5);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/opt/userhome/hadoop/apache-hive-2.3.5/conf";
        String version = "2.3.5";

        HiveCatalog catalog = new HiveCatalog(
            name,                    // catalog name
            defaultDatabase,         // default database
            hiveConfDir,             // Hive config (hive-site.xml) directory
            version                  // Hive version
        );
        tableEnv.registerCatalog(name, catalog);
        tableEnv.useCatalog(name);

        String createDbSql = "CREATE DATABASE IF NOT EXISTS myhive.flink";
        try {
            tableEnv.sqlUpdate(createDbSql);
        } catch (Exception e) {
            LOG.error("create database error:{}", e.getMessage());
        }

        String createTableSql = "CREATE TABLE myhive.flink.tob (\n" +
            "  `m` STRING,\n" +
            "  `ip` STRING,\n" +
            "  `logId` STRING,\n" +
            "  `status` STRING,\n" +
            "  `ts` STRING,\n" +
            "  procTime AS PROCTIME(),\n" +
            "  eventTime AS TO_TIMESTAMP(`ts`),\n" +
            "  WATERMARK FOR eventTime AS eventTime - INTERVAL '10' SECOND\n" +
            ") WITH (\n" +
            "  'connector.type' = 'kafka',\n" +
            "  'connector.version' = '0.11',\n" +
            "  'connector.topic' = 'tob_tag_log_json',\n" +
            "  'connector.startup-mode' = 'latest-offset',\n" +
            "  'connector.properties.zookeeper.connect' = '192.168.9.129:2181,192.168.9.130:2181,192.168.9.131:2181,192.168.9.132:2181,192.168.9.133:2181',\n" +
            "  'connector.properties.bootstrap.servers' = '192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092',\n" +
            "  'connector.properties.group.id' = 'flink_group_test_2',\n" +
            "  'format.type' = 'json',\n" +
            "  'format.derive-schema' = 'true',\n" +
            "  'update-mode' = 'append'\n" +
            ")";
        try {
            tableEnv.sqlUpdate(createTableSql);
        } catch (Exception e) {
            LOG.error("create table error:{}", e.getMessage());
        }

        String querySql = "SELECT `m`,\n" +
            "TUMBLE_START(`eventTime`, INTERVAL '60' SECOND) AS windowStart,\n" +
            "COUNT(*) AS number\n" +
            "FROM myhive.flink.tob\n" +
            "GROUP BY `m`, TUMBLE(eventTime, INTERVAL '60' SECOND)";

        Table result = tableEnv.sqlQuery(querySql);

        tableEnv.toAppendStream(result, Row.class).print().setParallelism(1);

        streamEnv.execute("flink sql test");
    }


}
