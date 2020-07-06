package com.ifchange.flink.log;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.flink.function.MyFilterFunction;
import com.ifchange.flink.sink.MyKafkaSink;
import com.ifchange.flink.util.ParamParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;


/**
 * data-app-tob 数据转换成json
 */
public class FlinkForDataAppTobParse {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkForDataAppTobParse.class);

    public static void main(String[] args) throws Exception {
        String topic = args[0];
        String groupId = args[1];
        final String topic2 = args[2];

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint 1 minute
//        environment.enableCheckpointing(1000 * 60);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint的周期, 每隔1000*60 ms进行启动一个检查点
        environment.getCheckpointConfig().setCheckpointInterval(1000 * 60);
        // 确保检查点之间有至少10000 ms的间隔【checkpoint最小间隔】
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // 同一时间只允许进行一个检查点
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        environment.getCheckpointConfig().setCheckpointTimeout(30000);
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        StateBackend fsStateBackend = new FsStateBackend("hdfs://dp/flink/checkpoints/databustob");
        environment.setStateBackend(fsStateBackend);
        //基于processTime不能做到完全实时
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //基于eventTime 需要设置waterMark水位线
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(5);

        //1、read data from kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092");
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        consumer011.setStartFromEarliest();
        consumer011.setStartFromGroupOffsets();
//        consumer011.setStartFromSpecificOffsets();
//        consumer011.setCommitOffsetsOnCheckpoints();

        DataStreamSource<String> stream = environment.addSource(consumer011);

        stream.filter(new MyFilterFunction())
            .uid("my-filter")
            .name("my-filter")
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
            }).uid("watermark")
            .name("watermark")
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String str) throws Exception {
                    String value = str.substring(str.indexOf("t="));
                    Map<String, String> map = ParamParseUtil.parse(value);
                    JSONObject json = new JSONObject();
                    json.put("ts", map.getOrDefault("t", ""));
                    json.put("m", map.getOrDefault("method", ""));
                    json.put("status", map.getOrDefault("status", ""));
                    json.put("logId", map.getOrDefault("logId", ""));
                    json.put("ip", map.getOrDefault("ip", ""));
                    //t=2020-05-21 15:11:58&ip=192.168.8.219&method=batch_query&id=2014961116112910403&message=&status=200&logId=A4718D1581F2A31CA33EED3DD9BD07A5&r=2&s=1
                    String result = JSONObject.toJSONString(json);
                    LOG.info("{}", result);
                    return new Tuple2<>(result, topic2);
                }
            }).uid("map-parse")
            .name("map-parse")
            .addSink(new MyKafkaSink());

        environment.execute("databustob data parse on flink");
    }


}
