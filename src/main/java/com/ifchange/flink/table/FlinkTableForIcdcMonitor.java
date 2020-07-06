//package com.ifchange.flink.table;
//
//import com.ifchange.flink.udf.UtcToLocal;
//import com.ifchange.flink.util.JavaMailUtil;
//import com.ifchange.flink.util.ParamParseUtil;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple6;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.Tumble;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.Nullable;
//import javax.mail.Address;
//import javax.mail.internet.InternetAddress;
//import java.sql.Timestamp;
//import java.util.Map;
//import java.util.Properties;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//
//
///**
// * flink table api
// * 1、汇总统计
// * 2、流量性能监控      请求量  w c m【1分钟】   平均响应时间 w c m【1分钟】  //写文件
// * 3、报警监控          失败次数   w c m 【1分钟 100次】
// * 超时数量   w c m【1分钟 100次】
// * 请求量     w c m【1分钟 3000次】
// * 4、单条报警          500
// */
//public class FlinkTableForIcdcMonitor {
//
//    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableForIcdcMonitor.class);
//
//    public static void main(String[] args) throws Exception {
//        String topic = args[0];
//        String groupId = args[1];
//
//        final Address[] receiveMails = new Address[]{
//            new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8")
//        };
//
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //checkpoint 1 minute
//        environment.enableCheckpointing(1000 * 60);
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        environment.setParallelism(4);
//
//        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment, fsSettings);
//
////        TableConfig tc = new TableConfig();
////        StreamTableEnvironment tableEnv = new StreamTableEnvironment(environment, tc);
//        //1、read data from kafka
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092");
//        props.setProperty("group.id", groupId);
//        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
//        DataStreamSource<String> stream = environment.addSource(consumer011);
//        DataStream<String> source = stream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return StringUtils.isNoneBlank(s);
//            }
//        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() { // 设置水位线
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
//        DataStream<MyIcdc> ds = source
//            .map(new MapFunction<String, MyIcdc>() {
//                @Override
//                public MyIcdc map(String value) throws Exception {
//                    MyIcdc myIcdc = new MyIcdc();
//                    LOG.info("{}", value);
//                    Map<String, String> map = ParamParseUtil.parse(value);
//                    String w = map.getOrDefault("w", "w");
//                    String c = map.getOrDefault("c", "c");
//                    String m = map.getOrDefault("m", "m");
//                    String wcm = String.format("%s->%s->%s", w, c, m);
//                    myIcdc.setWcm(wcm);
//                    //s 是否成功
//                    String s = map.getOrDefault("s", "0");
//                    int isSuccess = Integer.parseInt(s.trim());
//                    myIcdc.setSuccess(isSuccess);
//                    //r 频响时间
//                    String r = map.getOrDefault("r", "0");
//                    double responseTime = 0.0;
//                    try {
//                        responseTime = Double.parseDouble(r.trim());
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    myIcdc.setResponseTime(responseTime);
//                    String hostName = map.getOrDefault("hostname", "");
//                    myIcdc.setHostName(hostName);
//                    String errNo = map.getOrDefault("err_no", "");
//                    String t = map.get("t");
//                    String logId = map.getOrDefault("logid", "");
//                    String msg = map.getOrDefault("msg", "");
//                    String hostname = map.getOrDefault("hostname", "");
//
//                    //单条报警
//                    if (StringUtils.equals("0", s) && StringUtils.equals("500", errNo)) {
//                        //发送邮件
//                        String subject = "ICDC-JAVA日志报警";
//                        StringBuilder sb = new StringBuilder();
//                        sb.append("<h1>");
//                        sb.append(subject);
//                        sb.append("</h1>");
//                        sb.append("<table  border=\"1\">");
//
//                        sb.append("<tr>");
//                        sb.append("<td>");
//                        sb.append("时间");
//                        sb.append("</td>");
//                        sb.append("<td>");
//                        sb.append(t);
//                        sb.append("</td>");
//                        sb.append("</tr>");
//                        sb.append("<tr>");
//
//                        sb.append("<tr>");
//                        sb.append("<td>");
//                        sb.append("wcm");
//                        sb.append("</td>");
//                        sb.append("<td>");
//                        sb.append(wcm);
//                        sb.append("</td>");
//                        sb.append("</tr>");
//
//                        sb.append("<tr>");
//                        sb.append("<td>");
//                        sb.append("logid");
//                        sb.append("</td>");
//                        sb.append("<td>");
//                        sb.append(logId);
//                        sb.append("</td>");
//                        sb.append("</tr>");
//
//                        sb.append("<tr>");
//                        sb.append("<td>");
//                        sb.append("err_no");
//                        sb.append("</td>");
//                        sb.append("<td>");
//                        sb.append(errNo);
//                        sb.append("</td>");
//                        sb.append("</tr>");
//
//                        sb.append("<tr>");
//                        sb.append("<td>");
//                        sb.append("msg");
//                        sb.append("</td>");
//                        sb.append("<td>");
//                        sb.append(msg);
//                        sb.append("</td>");
//                        sb.append("</tr>");
//
//                        sb.append("<tr>");
//                        sb.append("<td>");
//                        sb.append("hostname");
//                        sb.append("</td>");
//                        sb.append("<td>");
//                        sb.append(hostname);
//                        sb.append("</td>");
//                        sb.append("</tr>");
//                        sb.append("</table>");
//                        String content = sb.toString();
//                        JavaMailUtil.sendEmail(receiveMails, subject, content);
//                    }
//                    return myIcdc;
//                }
//            });
//
//        // register the DataStream as table "icdc_monitor"
//        tableEnv.registerDataStream("icdc_monitor", ds, "rowtime.rowtime,wcm,hostName,responseTime,success");
//
//        //flink udf
//        tableEnv.registerFunction("utc2local", new UtcToLocal());
//
//        Table icdcMonitor = tableEnv.scan("icdc_monitor");
//
//        Table result = icdcMonitor
////            .window(Slide.over("1.minute").every("30.seconds").on("rowtime").as("w"))
//            .window(Tumble.over("1.minute").on("rowtime").as("w"))
//            .groupBy("w,wcm,hostName")
//            .select("wcm,hostName,responseTime.avg,count(1)," +
//                "utc2local(w.start) as start_time," +
//                "utc2local(w.end) as end_time");
//
//
//        DataStream<Tuple6<String, String, Double, Long, Timestamp, Timestamp>> tupleDataStream = tableEnv.toAppendStream(result,
//            Types.TUPLE(Types.STRING,
//                Types.STRING,
//                Types.DOUBLE,
//                Types.LONG,
//                Types.SQL_TIMESTAMP,
//                Types.SQL_TIMESTAMP));
//
//
//        //按小时划分文件夹
//        tupleDataStream.addSink(new BucketingSink<Tuple6<String, String,
//            Double, Long, Timestamp, Timestamp>>("hdfs://dp/flink/data/icdc")).setParallelism(1);
//
//
//        environment.execute("icdc monitor table on flink");
//
//
//
//    }
//
//}
