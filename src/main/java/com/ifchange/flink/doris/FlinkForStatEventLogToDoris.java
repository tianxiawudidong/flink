package com.ifchange.flink.doris;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.flink.mysql.Mysql;
import com.ifchange.flink.mysql.MysqlPool;
import com.ifchange.flink.sink.MyKafkaSink;
import com.ifchange.flink.util.LogUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 处理 kafka：stat-event-log 发送到
 */
public class FlinkForStatEventLogToDoris {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkForStatEventLogToDoris.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        //兩個topic
        String topics = args[0];
        String groupId = args[1];

        String[] topicArr = StringUtils.splitByWholeSeparator(topics, ",");
        List<String> topicList = Arrays.asList(topicArr);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint 1 minute
//        environment.enableCheckpointing(1000 * 60);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint的周期, 每隔1000*60 ms进行启动一个检查点
        environment.getCheckpointConfig().setCheckpointInterval(1000 * 60);
        // 确保检查点之间有至少1000 ms的间隔【checkpoint最小间隔】
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // 同一时间只允许进行一个检查点
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        environment.getCheckpointConfig().setCheckpointTimeout(60000);
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StateBackend fsStateBackend = new FsStateBackend("hdfs://dp/flink/checkpoints/stat-event-log");
        environment.setStateBackend(fsStateBackend);
        //基于processTime不能做到完全实时
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //基于eventTime 需要设置waterMark水位线
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(5);

        //1、read data from kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092");
        props.setProperty("group.id", groupId);
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topicList, new SimpleStringSchema(), props);

        environment.addSource(consumer011)
            .setParallelism(3)
            .uid("stat-event-log-kafka-consumer")
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    return StringUtils.isNoneBlank(s);
                }
            }).setParallelism(5)
            .map(new ProcessActiveUserFunction())
            .setParallelism(3)
            .uid("ai-interview-map")
            .name("ai-interview-map")
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    boolean flag = false;
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String keys = jsonObject.getString("keys");
                    String data = jsonObject.getString("data");
                    String[] keyArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(keys, ",");
                    String[] dataArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(data, "\t");
                    if (keyArray.length <= dataArray.length) {
                        flag = true;
                    } else {
                        LOG.error("key length:{} > value length:{}", keyArray.length, dataArray.length);
                        LOG.error("{}", s);
                    }
                    return flag;
                }
            })
            .uid("event-log-filter")
            .name("event-log-filter")
            .setParallelism(5)
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String s) throws Exception {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    //db name
                    String businessUnit = jsonObject.getString("businessUnit");
                    String projectName = jsonObject.getString("projectName");
                    //table=protocolName+version
                    String protocolName = jsonObject.getString("protocolName");
                    String version = "v" + jsonObject.getString("version");
                    String tableName = protocolName + "_" + version;
                    LOG.info("db:{} table:{}", businessUnit, tableName);
                    String keys = jsonObject.getString("keys");
                    String data = jsonObject.getString("data");
                    String createdAt = DTF.format(LocalDateTime.now());
                    String[] keyArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(keys, ",");
                    String[] dataArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(data, "\t");
                    StringBuilder sb = new StringBuilder();
                    sb.append(createdAt);
                    sb.append("\t");
                    if (null != keyArray && keyArray.length > 0) {
                        for (int i = 0; i < keyArray.length; i++) {
                            sb.append(dataArray[i]);
                            if (i != keyArray.length - 1) {
                                sb.append("\t");
                            }
                        }
                    }
                    String val = sb.toString();
                    LOG.info("{}", val);
                    String topic = businessUnit + "_" + tableName;
                    return new Tuple2<>(val, topic);
                }
            }).setParallelism(5)
            .uid("stat-event-log-map")
            .name("stat-event-log-map")
            .addSink(new MyKafkaSink())
            .uid("stat-event-log-kafka-producer")
            .name("stat-event-log-kafka-producer")
            .setParallelism(3);

        environment.execute("stat event log on flink");

    }

    static class ProcessActiveUserFunction extends RichMapFunction<String, String> {

        private MysqlPool mysqlPool3306;

        private ScheduledExecutorService scheduleExecutorService;

        private static final String USERNAME = "bi_user";

        private static final String PASSWORD = "7SvOaNaIJrE3oOUx";

        private static final String HOST = "192.168.9.33";

        @Override
        public void open(Configuration parameters) {
            scheduleExecutorService = Executors.newScheduledThreadPool(10);

            try {
                mysqlPool3306 = new MysqlPool(USERNAME, PASSWORD, HOST, 3306, "online_market");
            } catch (Exception e) {
                LOG.error("init mysql pool error:{}", e.getMessage());
                e.printStackTrace();
            }

            // mysql连接池 定时更新
            scheduleExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    LOG.info("-------------------------------------------------------");
                    LOG.info("execute scheduler task,close mysql pool,and re init....");
                    try {
                        mysqlPool3306.close();
                        mysqlPool3306 = new MysqlPool(USERNAME, PASSWORD, HOST, 3306, "online_market");
                    } catch (Exception e) {
                        LOG.info("InterviewMapFunctionV3.open 定时任务报错:{}", e.getMessage());
                        e.printStackTrace();
                    }
                    LOG.info("-------------------------------------------------------");
                }
            }, 6, 6, TimeUnit.HOURS);
        }

        @Override
        public String map(String s) throws Exception {
            Mysql mysql3306;
            try {
                mysql3306 = mysqlPool3306.getMysqlConn();
            } catch (Exception e) {
                LOG.info(LogUtils.getExceptionStackTrace(e));
                mysql3306 = mysqlPool3306.getMysqlConn();
            }
            try {
                JSONObject jsonObject = JSONObject.parseObject(s);
                //db name
                String businessUnit = jsonObject.getString("businessUnit");
                String projectName = jsonObject.getString("projectName");
                //table=protocolName+version
                String protocolName = jsonObject.getString("protocolName");
                String keys = jsonObject.getString("keys");
                String data = jsonObject.getString("data");
                if (StringUtils.equals(protocolName, "active_users")) {
                    Map<String, Integer> map = new HashMap<>();
                    String[] keyArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(keys, ",");
                    for (int i = 0; i < keyArray.length; i++) {
                        map.put(keyArray[i], i);
                    }
                    String[] dataArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(data, "\t");
                    String tidStr = dataArray[map.get("tid")];
                    String uid = dataArray[map.get("uid")];
                    String visitTime = dataArray[map.get("visit_time")];
                    String visitUrl = dataArray[map.get("visit_url")];
                    long tid = Long.parseLong(tidStr);
                    long uidNumber = Long.parseLong(uid);

                    if (tid > 20) {
                        if (StringUtils.equals(visitUrl, "/online/loginTob") || StringUtils.equals(visitUrl, "/online/logout")) {
                            //save login data to mysql
                            LOG.info("save log data to 3306.active_users");
                            String saveActiveUsers = "insert into `active_users`(`tid`,`uid`,`visit_url`,`visit_time`,`created_at`,`updated_at`)" +
                                " values(?,?,?,?,?,?)";
                            try {
                                int i = mysql3306.saveActiveUsers(saveActiveUsers, tid, uidNumber, visitUrl, visitTime);
                                LOG.info("save data to active_users :{}", i);
                            } catch (SQLException e) {
                                LOG.info("------------------------------------------------");
                                LOG.info("save data to active_user first time error:{}", e.getMessage());
                                try {
                                    mysql3306 = mysqlPool3306.getMysqlConn();
                                    int i = mysql3306.saveActiveUsers(saveActiveUsers, tid, uidNumber, visitUrl, visitTime);
                                    LOG.info("save data to active_users :{}", i);
                                } catch (Exception e1) {
                                    LOG.info(LogUtils.getExceptionStackTrace(e1));
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("process active_users error:{}", e.getMessage());
                e.printStackTrace();
            }

            try {
                mysqlPool3306.free(mysql3306);
            } catch (Exception e) {
                LOG.info("------------------------------------------------");
                LOG.info("mysql free mysql error:{}", e.getMessage());
                LOG.info(LogUtils.getExceptionStackTrace(e));
                LOG.info("------------------------------------------------");
            }
            return s;
        }

        @Override
        public void close() throws Exception {
            mysqlPool3306.close();
        }
    }


}
