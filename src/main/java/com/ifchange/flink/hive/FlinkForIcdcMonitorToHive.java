//package com.ifchange.flink.hive;
//
//import com.ifchange.flink.function.GetTuple5ByWCMFunction;
//import com.ifchange.flink.function.MyFilterFunction;
//import com.ifchange.flink.util.ParamParseUtil;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.api.java.tuple.Tuple7;
//import org.apache.flink.runtime.state.StateBackend;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.Nullable;
//import java.sql.Timestamp;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.Map;
//import java.util.Properties;
//
//
///**
// * icdc flink hive
// * 1、汇总统计     增加redis汇总统计 db【22】
// * 错误信息写入mysql【log_monitor】
// * 2、流量监控      请求量  w c m【1分钟】
// * 3、性能监控     平均响应时间 w c m【1分钟】
// * 4、报警监控      失败次数   w c m 【1分钟 100次】
// * 超时数量   w c m【1分钟 100次】
// * 请求量     w c m【1分钟 3000次】
// * 5、单条报警
// */
//public class FlinkForIcdcMonitorToHive {
//
//    private static final Logger LOG = LoggerFactory.getLogger(FlinkForIcdcMonitorToHive.class);
//
//    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
//
//    public static void main(String[] args) throws Exception {
//
//        String topic = args[0];
//        String groupId = args[1];
//
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        //checkpoint 1 minute
////        environment.enableCheckpointing(1000 * 60);
//        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 设置checkpoint的周期, 每隔1000*60 ms进行启动一个检查点
//        environment.getCheckpointConfig().setCheckpointInterval(1000 * 60);
//        // 确保检查点之间有至少1000 ms的间隔【checkpoint最小间隔】
//        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
//        // 同一时间只允许进行一个检查点
//        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
//        environment.getCheckpointConfig().setCheckpointTimeout(60000);
//        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
//        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
//
//        StateBackend fsStateBackend = new FsStateBackend("hdfs://dp/flink/checkpoints/icdc2");
//        environment.setStateBackend(fsStateBackend);
//        //基于processTime不能做到完全实时
////        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        //基于eventTime 需要设置waterMark水位线
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        environment.setParallelism(4);
//
//        /**
//         * constructor hiveCatalog
//         */
//        String name = "myhive";
//        String defaultDatabase = "default";
//        String hiveConfDir = "/opt/userhome/hadoop/apache-hive-2.3.5/conf";
//        String version = "2.3.4";
//
//        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment, fsSettings);
//        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//        tableEnv.registerCatalog(name, hiveCatalog);
//        tableEnv.useCatalog(name);
//
//        //1、read data from kafka
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092");
//        props.setProperty("group.id", groupId);
//        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
//        DataStreamSource<String> stream = environment.addSource(consumer011);
//
//        DataStream<String> source = stream.filter((FilterFunction<String>) StringUtils::isNoneBlank).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() { // 设置水位线
//            long currentMaxTimestamp = 0L;
//            long maxOutOfOrderness = 5000L;//最大允许的乱序时间是5s
//
//            @Override
//            public long extractTimestamp(String s, long l) {
//                Map<String, String> map = ParamParseUtil.parse(s);
//                String t = map.getOrDefault("t", "");
//                long timestamp = 0;
//                try {
//                    if (StringUtils.isNoneBlank(t)) {
//                        timestamp = Timestamp.valueOf(t).getTime();
//                    } else {
//                        LOG.info("extract watermark timestamp is null,s:{}", s);
//                        timestamp = System.currentTimeMillis();
//                    }
//                } catch (Exception e) {
//                    LOG.info("extract watermark timestamp error:{}", s);
//                    e.printStackTrace();
//                }
//                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
//                return timestamp;
//            }
//
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
//            }
//        });
//
//
//        //1、流量监控 性能监控
//        DataStream<Tuple5<String, Integer, Double, Integer, Integer>> monitor =
//            source.filter(new MyFilterFunction())
//                .map(new GetTuple5ByWCMFunction())
//                .keyBy(0)
//                .timeWindow(Time.minutes(1), Time.minutes(1))
//                .reduce(new ReduceFunction<Tuple5<String, Integer, Double, Integer, Integer>>() {
//                    @Override
//                    public Tuple5<String, Integer, Double, Integer, Integer> reduce(Tuple5<String, Integer, Double, Integer, Integer> t1, Tuple5<String, Integer, Double, Integer, Integer> t2) throws Exception {
//                        return new Tuple5<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3 + t2.f3, t1.f4 + t2.f4);
//                    }
//                }).map(new MapFunction<Tuple5<String, Integer, Double, Integer, Integer>, Tuple5<String, Integer, Double, Integer, Integer>>() {
//                @Override
//                public Tuple5<String, Integer, Double, Integer, Integer> map(Tuple5<String, Integer, Double, Integer, Integer> tuple5) throws Exception {
//                    String wcm = tuple5.f0;
//                    Integer callCount = tuple5.f1;
//                    Double responseTime = tuple5.f2;
//                    Integer failCount = tuple5.f3;
//                    Integer overTimeCount = tuple5.f4;
//                    Double avgTime = responseTime / callCount;
//                    return new Tuple5<>(wcm, callCount, avgTime, failCount, overTimeCount);
//                }
//            });
//
//        //3、汇总统计 redis 不使用窗口
////        data.timeWindowAll()
////        data.countWindowAll()
//        //非分组流  任务并发为1 且只能為1 The parallelism of non parallel operator must be 1
////        data.windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
////            .process(new IcdcWindows(22));
//
//        //4、报警监控
//        //4.1、请求量   w c m【1分钟 3000次】
//        //4.2、失败次数 w c m 【1分钟 100次】
//        //4.2、超時次数 w c m 【1分钟 100次】
//        DataStream<Tuple7<String, String, String, Integer, Double, Integer, Integer>> dd = monitor.map(
//            new MapFunction<Tuple5<String, Integer, Double, Integer, Integer>,
//                Tuple7<String, String, String, Integer, Double, Integer, Integer>>() {
//                @Override
//                public Tuple7<String, String, String, Integer, Double, Integer, Integer> map(Tuple5<String, Integer, Double, Integer, Integer> tuple5) throws Exception {
//                    String wcm = tuple5.f0;
//                    Integer totalCount = tuple5.f1;
//                    Double avgTime = tuple5.f2;
//                    Integer failCount = tuple5.f3;
//                    Integer overTimeCount = tuple5.f4;
//                    LocalDateTime localDateTime = LocalDateTime.now();
//                    LocalDateTime before = localDateTime.minusMinutes(1L);
//                    String from = DTF.format(before);
//                    String to = DTF.format(localDateTime);
//                    LOG.info("time:{}->{},wcm:{},请求数量:{},平均响应时间:{},失败数量:{},超时数量:{}",
//                        from, to, wcm, totalCount, avgTime, failCount, overTimeCount);
//                    return new Tuple7<>(from, to, wcm, totalCount, avgTime, failCount, overTimeCount);
//                }
//            });
//
//        tableEnv.registerDataStream("dd", dd, "from_time,end_time,wcm,total_count,avg_time,fail_count,over_time_count");
//
//        String sql = "insert into icdc_flink_hive_test(from_time,end_time,wcm,total_count,avg_time,fail_count,over_time_count) " +
//            "as select from_time,end_time,wcm,total_count,avg_time,fail_count,over_time_count from dd";
//        LOG.info(sql);
//        // union the two tables
//        tableEnv.execute(sql);
//
//        dd.print();
//
//        environment.execute("icdc monitor to hive on flink");
//    }
//
//
//}
