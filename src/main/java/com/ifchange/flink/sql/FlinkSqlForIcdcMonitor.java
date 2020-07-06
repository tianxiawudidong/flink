//package com.ifchange.flink.sql;
//
//import com.ifchange.flink.udf.UtcToLocal;
//import com.ifchange.flink.util.JavaMailUtil;
//import com.ifchange.flink.util.ParamParseUtil;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.*;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableConfig;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.Nullable;
//import javax.mail.Address;
//import javax.mail.internet.InternetAddress;
//import java.sql.Timestamp;
//import java.util.Map;
//import java.util.Properties;
//
//
///**
// * 1、汇总统计
// * 2、流量性能监控      请求量  w c m【1分钟】   平均响应时间 w c m【1分钟】  //写文件
// * 3、报警监控          失败次数   w c m 【1分钟 100次】
// * 超时数量   w c m【1分钟 100次】
// * 请求量     w c m【1分钟 3000次】
// * 4、单条报警          500
// */
//public class FlinkSqlForIcdcMonitor {
//
//    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlForIcdcMonitor.class);
//
////    public static void main(String[] args) throws Exception {
////        String topic = args[0];
////        String groupId = args[1];
////
////        final Address[] receiveMails = new Address[]{
////            new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8")
////        };
////
////        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
////
////        //checkpoint 1 minute
////        environment.enableCheckpointing(1000 * 60);
////        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
////        environment.setParallelism(4);
////
////        TableConfig tc = new TableConfig();
////        StreamTableEnvironment tableEnv = new StreamTableEnvironment(environment, tc);
////        //1、read data from kafka
////        Properties props = new Properties();
////        props.setProperty("bootstrap.servers", "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092");
////        props.setProperty("group.id", groupId);
////        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
////        DataStreamSource<String> stream = environment.addSource(consumer011);
////        DataStream<String> source = stream.filter(new FilterFunction<String>() {
////            @Override
////            public boolean filter(String s) throws Exception {
////                return StringUtils.isNoneBlank(s);
////            }
////        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() { // 设置水位线
////            long currentMaxTimestamp = 0L;
////            long maxOutOfOrderness = 5000L;//最大允许的乱序时间是5s
////
////            @Override
////            public long extractTimestamp(String s, long l) {
////                Map<String, String> map = ParamParseUtil.parse(s);
////                String t = map.getOrDefault("t", "");
////                long timestamp = 0;
////                try {
////                    if (StringUtils.isNoneBlank(t)) {
////                        timestamp = Timestamp.valueOf(t).getTime();
////                    } else {
////                        LOG.info("extract watermark timestamp is null,s:{}", s);
////                        timestamp = System.currentTimeMillis();
////                    }
////                } catch (Exception e) {
////                    LOG.info("extract watermark timestamp error:{}", s);
////                    e.printStackTrace();
////                }
////                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
////                return timestamp;
////            }
////
////            @Nullable
////            @Override
////            public Watermark getCurrentWatermark() {
////                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
////            }
////        });
////
////        DataStream<Tuple4<String, String, Double, Integer>> ds = source
////            .map(new MapFunction<String, Tuple4<String, String, Double, Integer>>() {
////                @Override
////                public Tuple4<String, String, Double, Integer> map(String value) throws Exception {
////                    LOG.info("{}", value);
////                    Map<String, String> map = ParamParseUtil.parse(value);
////                    String w = map.getOrDefault("w", "w");
////                    String c = map.getOrDefault("c", "c");
////                    String m = map.getOrDefault("m", "m");
////                    String wcm = String.format("%s->%s->%s", w, c, m);
////                    //s 是否成功
////                    String s = map.getOrDefault("s", "0");
////                    int isSuccess = Integer.parseInt(s.trim());
////                    //r 频响时间
////                    String r = map.getOrDefault("r", "0");
////                    double responseTime = 0.0;
////                    try {
////                        responseTime = Double.parseDouble(r.trim());
////                    } catch (Exception e) {
////                        e.printStackTrace();
////                    }
////
////                    String hostName = map.getOrDefault("hostname", "");
////                    String errNo = map.getOrDefault("err_no", "");
////                    String t = map.get("t");
////                    String logId = map.getOrDefault("logid", "");
////                    String msg = map.getOrDefault("msg", "");
////                    String hostname = map.getOrDefault("hostname", "");
////
////                    //单条报警
////                    if (StringUtils.equals("0", s) && StringUtils.equals("500", errNo)) {
////                        //发送邮件
////                        String subject = "ICDC-JAVA日志报警";
////                        StringBuilder sb = new StringBuilder();
////                        sb.append("<h1>");
////                        sb.append(subject);
////                        sb.append("</h1>");
////                        sb.append("<table  border=\"1\">");
////
////                        sb.append("<tr>");
////                        sb.append("<td>");
////                        sb.append("时间");
////                        sb.append("</td>");
////                        sb.append("<td>");
////                        sb.append(t);
////                        sb.append("</td>");
////                        sb.append("</tr>");
////                        sb.append("<tr>");
////
////                        sb.append("<tr>");
////                        sb.append("<td>");
////                        sb.append("wcm");
////                        sb.append("</td>");
////                        sb.append("<td>");
////                        sb.append(wcm);
////                        sb.append("</td>");
////                        sb.append("</tr>");
////
////                        sb.append("<tr>");
////                        sb.append("<td>");
////                        sb.append("logid");
////                        sb.append("</td>");
////                        sb.append("<td>");
////                        sb.append(logId);
////                        sb.append("</td>");
////                        sb.append("</tr>");
////
////                        sb.append("<tr>");
////                        sb.append("<td>");
////                        sb.append("err_no");
////                        sb.append("</td>");
////                        sb.append("<td>");
////                        sb.append(errNo);
////                        sb.append("</td>");
////                        sb.append("</tr>");
////
////                        sb.append("<tr>");
////                        sb.append("<td>");
////                        sb.append("msg");
////                        sb.append("</td>");
////                        sb.append("<td>");
////                        sb.append(msg);
////                        sb.append("</td>");
////                        sb.append("</tr>");
////
////                        sb.append("<tr>");
////                        sb.append("<td>");
////                        sb.append("hostname");
////                        sb.append("</td>");
////                        sb.append("<td>");
////                        sb.append(hostname);
////                        sb.append("</td>");
////                        sb.append("</tr>");
////                        sb.append("</table>");
////                        String content = sb.toString();
////                        JavaMailUtil.sendEmail(receiveMails, subject, content);
////                    }
////                    return new Tuple4<>(wcm, hostName, responseTime, isSuccess);
////                }
////            });
////
////        // ingest a DataStream from an external source
////        // register the DataStream as table "icdc_monitor"
////        tableEnv.registerDataStream("icdc_monitor", ds,
////            "wcm,host_name,response_time,is_success,proctime.proctime,rowtime.rowtime");
////
////        //flink udf
////        tableEnv.registerFunction("utc2local", new UtcToLocal());
////
////
////        //基于partition group by 不是全局的
////        //窗口  分钟
////        Table result = tableEnv.sqlQuery(
////            "SELECT wcm,host_name,avg(response_time) as avg_time,count(1) as total_num, " +
////                "utc2local(TUMBLE_START(rowtime, INTERVAL '1' MINUTE)) as start_time, " +
////                "utc2local(TUMBLE_END(rowtime, INTERVAL '1' MINUTE)) as end_time " +
////                "FROM icdc_monitor " +
////                "GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), wcm,host_name");
////
////
////        DataStream<Tuple6<String, String, Double, Long, Timestamp, Timestamp>> tupleDataStream = tableEnv.toAppendStream(result,
////            Types.TUPLE(Types.STRING,
////                Types.STRING,
////                Types.DOUBLE,
////                Types.LONG,
////                Types.SQL_TIMESTAMP,
////                Types.SQL_TIMESTAMP));
////
////        tupleDataStream.print();
////
////        //CsvTableSink 用在无限流的场景下的话，不会马上写出去，会 buffer 起来，在结束的时候会 flush 出去。所以一般常用在测试场景。
////        // 在真实线上的话，建议用 StreamingFileSink 或者 RollingFileSink。
//////        TableSink sink = new CsvTableSink("hdfs://dp/flink/icdc_result", ",");
////
//////        String[] fieldNames = {"wcm", "host_name", "avg_time", "total_num", "start_time"};
//////        TypeInformation[] fieldTypes = {Types.STRING,
//////            Types.STRING,
//////            Types.DOUBLE,
//////            Types.LONG,
//////            Types.SQL_TIMESTAMP};
//////        tableEnv.registerTableSink("results", fieldNames, fieldTypes, sink);
//////        result.insertInto("results");
////
////
////        //窗口 day
//////        Table result2 = tableEnv.sqlQuery(
//////            "SELECT wcm,host_name,avg(response_time) as avg_time,count(1) as total_num, " +
//////                "utc2local(TUMBLE_START(rowtime, INTERVAL '1' DAY)) as start_time, " +
//////                "utc2local(TUMBLE_END(rowtime, INTERVAL '1' DAY)) as end_time  " +
//////                "FROM icdc_monitor " +
//////                "GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), wcm,host_name");
////////
//////        TableSink sink = new CsvTableSink("hdfs://dp/flink/icdc_result", ",");
//////
//////        String[] fieldNames = {"wcm", "host_name", "avg_time", "total_num", "start_time", "end_time"};
//////        TypeInformation[] fieldTypes = {Types.STRING,
//////            Types.STRING,
//////            Types.DOUBLE,
//////            Types.LONG,
//////            Types.SQL_TIMESTAMP,
//////            Types.SQL_TIMESTAMP};
//////        tableEnv.registerTableSink("result", fieldNames, fieldTypes, sink);
//////        result2.insertInto("result");
////
//////        CsvTableSink
////
////        environment.execute("icdc monitor sql on flink");
//
//        // compute SUM(amount) per day (in processing-time)
////        Table result2 = tableEnv.sqlQuery(
////            "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user");
////
////        // compute every hour the SUM(amount) of the last 24 hours in event-time
////        Table result3 = tableEnv.sqlQuery(
////            "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product");
////
////        // compute SUM(amount) per session with 12 hour inactivity gap (in event-time)
////        Table result4 = tableEnv.sqlQuery(
////            "SELECT user, " +
////                "  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart, " +
////                "  SESSION_ROWTIME(rowtime, INTERVAL '12' HOUR) AS snd, " +
////                "  SUM(amount) " +
////                "FROM Orders " +
////                "GROUP BY SESSION(rowtime, INTERVAL '12' HOUR), user");
//
//
////        //执行sql查询     滚动窗口 60秒    计算60秒窗口内wcm、请求数量、平均响应时间、失败数量
////        String sql = "SELECT TUMBLE_END(proctime, INTERVAL '30' SECOND) as process_time,"
////            + "wcm,sum(call_count) as success_number,sum(fail_count) as fail_number "
////            + "FROM icdc_java "
////            + "GROUP BY TUMBLE(proctime, INTERVAL '30' SECOND),wcm";
////        LOG.info("sql:{}", sql);
////        Table sqlQuery = tableEnv.sqlQuery(sql);
////
////        //Table 转化为 DataStream
////        DataStream<Tuple4<Timestamp, String, Long, Long>> appendStream = tableEnv.toAppendStream(sqlQuery,
////            Types.TUPLE(Types.SQL_TIMESTAMP, Types.STRING, Types.LONG, Types.LONG));
//
////    }
//
//}
