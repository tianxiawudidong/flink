package com.ifchange.flink.log;

import com.ifchange.flink.function.GetTuple5ByMFunction;
import com.ifchange.flink.function.MyFilterFunction;
import com.ifchange.flink.function.MyMapV2Function;
import com.ifchange.flink.util.JavaMailUtil;
import com.ifchange.flink.util.ParamParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.mail.Address;
import javax.mail.internet.InternetAddress;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;


/**
 * data-app-tob 数据流监控
 * 1、汇总统计
 * 增加redis汇总统计 db【23】
 * 超时写入mysql【log_monitor】
 * 2、流量监控
 * 请求量  m【1分钟】
 * 3、性能监控
 * 平均响应时间 m【1分钟】
 * 4、报警监控
 * 失败次数  m 【1分钟 100次】
 * 超时次数         m 【1分钟 10次】
 * 请求量           m【1分钟 3000次】
 * 5、单条报警
 */
public class FlinkForDataAppTobMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkForDataAppTobMonitor.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private static final String DATABUSTOB_SUCCESS = "databustob_success";

    private static final String DATABUSTOB_FAIL = "databustob_fail";

    public static void main(String[] args) throws Exception {
        String topic = args[0];
        String groupId = args[1];

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint 1 minute
//        environment.enableCheckpointing(1000 * 60);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint的周期, 每隔5000*60 ms进行启动一个检查点
        environment.getCheckpointConfig().setCheckpointInterval(1000 * 60 * 5);
        // 确保检查点之间有至少1000 ms的间隔【checkpoint最小间隔】
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        // 同一时间只允许进行一个检查点
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        environment.getCheckpointConfig().setCheckpointTimeout(60000);
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        StateBackend fsStateBackend = new FsStateBackend("hdfs://dp/flink/checkpoints/databustob");
        environment.setStateBackend(fsStateBackend);
        //基于processTime不能做到完全实时
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //基于eventTime 需要设置waterMark水位线
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(5);

        final Address[] receiveMails = new Address[]{
            new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"),
            new InternetAddress("xuxu@ifchange.com", "", "UTF-8")
        };

        //1、read data from kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092");
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        DataStreamSource<String> stream = environment.addSource(consumer011);

        DataStream<String> source = stream.filter(new MyFilterFunction())
            .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() { // 设置水位线
                long currentMaxTimestamp = 0L;
                long maxOutOfOrderness = 5000L;//最大允许的乱序时间是5s

                @Override
                public long extractTimestamp(String s, long l) {
                    String data = s.substring(s.indexOf("t="));
                    Map<String, String> map = ParamParseUtil.parse(data);
                    String t = map.getOrDefault("t", "");
                    long timestamp = 0;
                    try {
                        if (StringUtils.isNoneBlank(t)) {
                            timestamp = Timestamp.valueOf(t).getTime();
                        } else {
                            LOG.info("extract watermark timestamp is null,s:{}", s);
                            timestamp = System.currentTimeMillis();
                        }
                    } catch (Exception e) {
                        LOG.info("extract watermark timestamp error:{}", s);
                        e.printStackTrace();
                    }
                    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                    return timestamp;
                }

                @Nullable
                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                }
            });

        //1、汇总统计 redis
        DataStream<String> result = source.filter(new MyFilterFunction())
            .uid("databustob-filter")
            .name("databustob-filter")
            .setParallelism(3)
            .map(new MapFunction<String, String>() {
                @Override
                public String map(String str) throws Exception {
                    String value = str.substring(str.indexOf("t="));
                    Map<String, String> map = ParamParseUtil.parse(value);
                    String t = map.get("t");
                    if (map.containsKey("algorithms")) {
                        String algorithms = map.get("algorithms");
                        String id = map.getOrDefault("id", "");
                        String msg = map.getOrDefault("err_msg", "");
                        String result = String.format("algorithms:%s,id:%s,err_msg:%s", algorithms, id, msg);
                        LOG.info("{}", result);
                        //发送邮件
                        String subject = "数据流【TOB】超时报警";
                        StringBuilder sb = new StringBuilder();
                        sb.append("<h1>");
                        sb.append(subject);
                        sb.append("</h1>");
                        sb.append("<table  border=\"1\">");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("时间");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(t);
                        sb.append("</td>");
                        sb.append("</tr>");
                        sb.append("<tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("algorithms");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(algorithms);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("id");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(id);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("err_msg");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(msg);
                        sb.append("</td>");
                        sb.append("</tr>");
                        sb.append("</table>");
                        String content = sb.toString();
                        JavaMailUtil.sendEmail(receiveMails, subject, content);
                    }
                    return str;
                }
            }).map(new DataAppTobProcessMapFunction(23))
            .uid("redis-count-map")
            .name("redis-count-map")
            .setParallelism(5)
            .map(new GetTuple5ByMFunction())
            .keyBy(0)
            .timeWindow(Time.minutes(1), Time.minutes(1))
            .reduce(new ReduceFunction<Tuple5<String, Integer, Double, Integer, Integer>>() {
                @Override
                public Tuple5<String, Integer, Double, Integer, Integer> reduce(Tuple5<String, Integer, Double, Integer, Integer> t1, Tuple5<String, Integer, Double, Integer, Integer> t2) throws Exception {
                    return new Tuple5<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3 + t2.f3, t1.f4 + t2.f4);
                }
            }).map(new MapFunction<Tuple5<String, Integer, Double, Integer, Integer>, Tuple5<String, Integer, Double, Integer, Integer>>() {
                @Override
                public Tuple5<String, Integer, Double, Integer, Integer> map(Tuple5<String, Integer, Double, Integer, Integer> tuple5) throws Exception {
                    String method = tuple5.f0;
                    Integer callCount = tuple5.f1;
                    Double responseTime = tuple5.f2;
                    Integer failCount = tuple5.f3;
                    Integer overTimeCount = tuple5.f4;
                    Double avgTime = responseTime / callCount;
                    return new Tuple5<>(method, callCount, avgTime, failCount, overTimeCount);
                }
            }).map(new MapFunction<Tuple5<String, Integer, Double, Integer, Integer>, String>() {
                @Override
                public String map(Tuple5<String, Integer, Double, Integer, Integer> tuple5) throws Exception {
                    String method = tuple5.f0;
                    Integer totalCount = tuple5.f1;
                    Double avgTime = tuple5.f2;
                    Integer failCount = tuple5.f3;
                    Integer overTimeCount = tuple5.f4;
                    LocalDateTime localDateTime = LocalDateTime.now();
                    LocalDateTime before = localDateTime.minusMinutes(1L);
                    String from = DTF.format(before);
                    String to = DTF.format(localDateTime);
                    LOG.info("time:{}->{},wcm:{},请求数量:{},平均响应时间:{},失败数量:{},超时数量:{}",
                        from, to, method, totalCount, avgTime, failCount, overTimeCount);
                    if (totalCount >= 5000 || failCount >= 100 || overTimeCount >= 200) {

                        String time = "[" + from + "-" + to + "]";
                        String reason;
                        String subject;
                        if (totalCount >= 5000) {
                            reason = "1分钟内请求数量{" + totalCount + "}大于5000";
                            subject = "数据流【TOB】流量报警";
                        } else if (failCount >= 100) {
                            reason = "1分钟内失败次数:{" + failCount + "}大于100";
                            subject = "数据流【TOB】失败报警";
                        } else {
                            reason = "1分钟内超时次数:{" + overTimeCount + "}大于200";
                            subject = "数据流【TOB】超时报警";
                        }
                        //发送邮件
                        StringBuilder sb = new StringBuilder();
                        sb.append("<h1>");
                        sb.append(subject);
                        sb.append("</h1>");
                        sb.append("<table  border=\"1\">");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("时间");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(time);
                        sb.append("</td>");
                        sb.append("</tr>");
                        sb.append("<tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("方法名");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(method);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("报警原因");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(reason);
                        sb.append("</td>");
                        sb.append("</tr>");
                        sb.append("</table>");
                        String content = sb.toString();
                        JavaMailUtil.sendEmail(receiveMails, subject, content);
                    }
                    return method;
                }
            }).uid("time-window-monitor")
            .name("time-window-monitor")
            .setParallelism(3);

        result.print();

        environment.execute("databustob monitor on flink");
    }

    static class DataAppTobProcessMapFunction extends MyMapV2Function {

        public DataAppTobProcessMapFunction(int index) {
            super(index);
        }

        @Override
        public String map(String str) throws Exception {
            String value = str.substring(str.indexOf("t="));
            Map<String, String> map = ParamParseUtil.parse(value);
            String method = map.getOrDefault("method", "m");
            String time = map.getOrDefault("t", "");
            //s 是否成功
            String s = map.getOrDefault("s", "0");
            //r 频响时间
            String r = map.getOrDefault("r", "0");
            int responseTime = 0;
            try {
                responseTime = Integer.parseInt(r.trim());
            } catch (Exception e) {
                e.printStackTrace();
            }
            //r 频响时间
            if (StringUtils.isNotBlank(time)) {
                String day = time.split(" ")[0];
                String successKey = DATABUSTOB_SUCCESS + "-" + day;
                String failKey = DATABUSTOB_FAIL + "-" + day;
                String methodSuccessKey = method + "-success-" + day;
                String methodFailKey = method + "-fail-" + day;
                //统计wcm每次的时间[0,1)秒
                String methodTimeKey1 = method + "-" + day + "-time1";
                //[1,5)秒
                String methodTimeKey2 = method + "-" + day + "-time2";
                //[5,+)秒
                String methodTimeKey3 = method + "-" + day + "-time3";
                //redis统计次数
                if ("1".equals(s)) {
                    redis.incr(successKey);
                    redis.incr(methodSuccessKey);
                } else {
                    redis.incr(failKey);
                    redis.incr(methodFailKey);
                }
                //responseTime 毫秒
                if (responseTime >= 0 && responseTime < 1000) { //[0,1)
                    redis.incr(methodTimeKey1);
                } else if (responseTime >= 1000 && responseTime < 5000) { //[1,5)
                    redis.incr(methodTimeKey2);
                } else { //[5,+)
                    redis.incr(methodTimeKey3);
                }
                //将超时的记录  写到mysql
//                t={}&id={}&algorithms=crop_tag&err_msg={}
//                if (map.containsKey("algorithms")) {
//                    String functionMark = map.get("algorithms");
//                    String f = "data-app-tob";
//                    String c = "tob_tag";
//                    String msg = map.getOrDefault("err_msg", "");
//                    String id = map.getOrDefault("id", "");
//                    String sql = "insert into log_monitor(`time`,`function_mark`,`is_success`,`response_time`," +
//                        "`work_name`,`alarm_reason`,`type`,`f`,`c`,`m`) "
//                        + "values(\"" + time + "\",\"" + functionMark + "\"," + s + ","
//                        + r + ",\"" + id + "\"," + "\"" + msg + "\"," + 0 + ",\"" + f + "\",\"" + c + "\",\"" + method + "\"" + ")";
//                    LOG.info("{}", sql);
//                    PreparedStatement pstmt = connection.prepareStatement(sql);
//                    try {
//                        pstmt.executeUpdate(sql);
//                        LOG.info("insert mysql success");
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        LOG.info("insert mysql fail,msg:{}", e.getMessage());
//                    }
//                    pstmt.close();
//                }
            }
            return str;
        }

    }

}
