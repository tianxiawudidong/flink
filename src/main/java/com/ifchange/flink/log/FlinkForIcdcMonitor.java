package com.ifchange.flink.log;

import com.ifchange.flink.function.*;
import com.ifchange.flink.util.JavaMailUtil;
import com.ifchange.flink.util.ParamParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
 * 1、汇总统计     增加redis汇总统计 db【22】
 * 错误信息写入mysql【log_monitor】
 * 2、流量监控      请求量  w c m【1分钟】
 * 3、性能监控     平均响应时间 w c m【1分钟】
 * 4、报警监控      失败次数   w c m 【1分钟 100次】
 * 超时数量   w c m【1分钟 100次】
 * 请求量     w c m【1分钟 3000次】
 * 5、单条报警
 */
public class FlinkForIcdcMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkForIcdcMonitor.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private static final String ICDC_SUCCESS = "icdc_success";

    private static final String ICDC_FAIL = "icdc_fail";

    public static void main(String[] args) throws Exception {
        String topic = args[0];
        String groupId = args[1];

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
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        StateBackend fsStateBackend = new FsStateBackend("hdfs://dp/flink/checkpoints/icdc");
        environment.setStateBackend(fsStateBackend);
        //基于processTime不能做到完全实时
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //基于eventTime 需要设置waterMark水位线
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(4);

        final Address[] receiveMails = new Address[]{
            new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"),
            new InternetAddress("xuxu@ifchange.com", "", "UTF-8")
        };

        //1、read data from kafka
        Properties props = new Properties();
        //flink.partition-discovery.interval-millis
        props.setProperty("bootstrap.servers", "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092");
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        DataStreamSource<String> stream = environment.addSource(consumer011);

        DataStream<String> source = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return StringUtils.isNoneBlank(s);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() { // 设置水位线
            long currentMaxTimestamp = 0L;
            long maxOutOfOrderness = 5000L;//最大允许的乱序时间是5s

            @Override
            public long extractTimestamp(String s, long l) {
                Map<String, String> map = ParamParseUtil.parse(s);
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


        DataStream<String> result = source.filter(new MyFilterFunction())
            //1、单条报警
            .map(new MapFunction<String, String>() {
                @Override
                public String map(String value) throws Exception {
                    Map<String, String> map = ParamParseUtil.parse(value);
                    //t=2019-05-24 11:33:46&w=icdc_online&c=resumes/logic_contact&m=get_multi_contacts_by_ids&logid=&signid=5ce7655065408&r=26&s=1&err_no=0&msg=success&hostname=search9129.echeng
                    String t = map.get("t");
                    String w = map.getOrDefault("w", "w");
                    String c = map.getOrDefault("c", "c");
                    String m = map.getOrDefault("m", "m");
                    String wcm = String.format("%s,%s,%s", w, c, m);
                    String s = map.getOrDefault("s", "0");
                    String logId = map.getOrDefault("logid", "");
                    String msg = map.getOrDefault("msg", "");
                    String errNo = map.getOrDefault("err_no", "");
                    String hostname = map.getOrDefault("hostname", "");
                    if (StringUtils.equals("0", s) && StringUtils.equals("500", errNo)) {
                        //发送邮件
                        String subject = "ICDC-JAVA日志报警";
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
                        sb.append("wcm");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(wcm);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("logid");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(logId);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("err_no");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(errNo);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("msg");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(msg);
                        sb.append("</td>");
                        sb.append("</tr>");

                        sb.append("<tr>");
                        sb.append("<td>");
                        sb.append("hostname");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(hostname);
                        sb.append("</td>");
                        sb.append("</tr>");
                        sb.append("</table>");
                        String content = sb.toString();
                        JavaMailUtil.sendEmail(receiveMails, subject, content);
                    }
                    return value;
                }
            })
            .uid("single-monitor-map")
            .name("single-monitor-map")
            .setParallelism(5)
            //2、汇总统计 redis
            .map(new IcdcProcessMapFunction(22))
            .uid("redis-count-map")
            .name("redis-count-map")
            .setParallelism(5)
            //3、流量监控 性能监控
            .map(new GetTuple5ByWCMFunction())
            .name("get-wcm-map")
            .uid("get-wcm-map")
            .setParallelism(5)
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
                    String wcm = tuple5.f0;
                    Integer callCount = tuple5.f1;
                    Double responseTime = tuple5.f2;
                    Integer failCount = tuple5.f3;
                    Integer overTimeCount = tuple5.f4;
                    Double avgTime = responseTime / callCount;
                    return new Tuple5<>(wcm, callCount, avgTime, failCount, overTimeCount);
                }
            }).uid("key-map")
            .name("key-map")
            .setParallelism(5)
            .map(new MapFunction<Tuple5<String, Integer, Double, Integer, Integer>, String>() {
                @Override
                public String map(Tuple5<String, Integer, Double, Integer, Integer> tuple5) throws Exception {
                    String wcm = tuple5.f0;
                    Integer totalCount = tuple5.f1;
                    Double avgTime = tuple5.f2;
                    Integer failCount = tuple5.f3;
                    Integer overTimeCount = tuple5.f4;
                    LocalDateTime localDateTime = LocalDateTime.now();
                    LocalDateTime before = localDateTime.minusMinutes(1L);
                    String from = DTF.format(before);
                    String to = DTF.format(localDateTime);
                    LOG.info("time:{}->{},wcm:{},请求数量:{},平均响应时间:{},失败数量:{},超时数量:{}",
                        from, to, wcm, totalCount, avgTime, failCount, overTimeCount);
                    if (totalCount >= 3000 || failCount >= 100 || overTimeCount >= 100) {

                        String time = "[" + from + "-" + to + "]";
                        String reason;
                        if (totalCount >= 3000) {
                            reason = "1分钟内请求数量{" + totalCount + "}大于3000";
                        } else if (failCount >= 100) {
                            reason = "1分钟内失败次数:{" + failCount + "}大于100";
                        } else {
                            reason = "1分钟内超时次数:{" + overTimeCount + "}大于100";
                        }
                        //发送邮件
                        String subject = "ICDC-JAVA日志报警";
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
                        sb.append("wcm");
                        sb.append("</td>");
                        sb.append("<td>");
                        sb.append(wcm);
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
                    return wcm;
                }
            }).uid("time-window-monitor")
            .name("time-window-monitor")
            .setParallelism(2);

        result.print();

        environment.execute("icdc monitor on flink");
    }

    static class IcdcProcessMapFunction extends MyMapV2Function {

        public IcdcProcessMapFunction(int index) {
            super(index);
        }

        @Override
        public String map(String value) throws Exception {
            Map<String, String> map = ParamParseUtil.parse(value);
            String w = map.getOrDefault("w", "w");
            String c = map.getOrDefault("c", "c");
            String m = map.getOrDefault("m", "m");
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
            String hostName = map.getOrDefault("hostname", "");
            //r 频响时间
            if (StringUtils.isNotBlank(time)) {
                String day = time.split(" ")[0];
                String successKey = ICDC_SUCCESS + "-" + day;
                String failKey = ICDC_FAIL + "-" + day;
                String workSuccessKey = w + "-success-" + day;
                String workFailKey = w + "-fail-" + day;
                //统计具体work+c+m
                String wcmSuccessKey = w + "-" + c + "-" + m + "-success-" + day;
                String wcmFailKey = w + "-" + c + "-" + m + "-fail-" + day;
                //统计wcm每次的时间[0,1)秒
                String wcmTimeKey1 = w + "-" + c + "-" + m + "-" + day + "-time1";
                //[1,5)秒
                String wcmTimeKey2 = w + "-" + c + "-" + m + "-" + day + "-time2";
                //[5,+)秒
                String wcmTimeKey3 = w + "-" + c + "-" + m + "-" + day + "-time3";
                String host129Key = "icdc-search9129-" + day;
                String host130Key = "icdc-search9130-" + day;
                String host131Key = "icdc-search9131-" + day;
                String host132Key = "icdc-search9132-" + day;
                String host133Key = "icdc-search9133-" + day;
                String host28Key = "icdc-search28-" + day;
                if (hostName.startsWith("search9129")) redis.incr(host129Key);
                else if (hostName.startsWith("search9130")) redis.incr(host130Key);
                else if (hostName.startsWith("search9131")) redis.incr(host131Key);
                else if (hostName.startsWith("search9132")) redis.incr(host132Key);
                else if (hostName.startsWith("search9133")) redis.incr(host133Key);
                else if (hostName.startsWith("search28")) redis.incr(host28Key);
                else LOG.info("hostname:{} is not correct", hostName);
                //redis统计次数
                if ("1".equals(s)) {
                    redis.incr(successKey);
                    redis.incr(workSuccessKey);
                    redis.incr(wcmSuccessKey);
                } else {
                    redis.incr(failKey);
                    redis.incr(workFailKey);
                    redis.incr(wcmFailKey);
                }
                //responseTime 毫秒
                if (responseTime >= 0 && responseTime < 1000) { //[0,1)
                    redis.incr(wcmTimeKey1);
                } else if (responseTime >= 1000 && responseTime < 5000) { //[1,5)
                    redis.incr(wcmTimeKey2);
                } else { //[5,+)
                    redis.incr(wcmTimeKey3);
                }
            }
            return value;
        }

    }

}
