package com.ifchange.flink.state;

import com.ifchange.flink.function.MyFilterFunction;
import com.ifchange.flink.util.ParamParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;


/**
 * flink state test
 */
public class FlinkForIcdcMonitorState {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkForIcdcMonitorState.class);

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

        StateBackend fsStateBackend = new FsStateBackend("hdfs://dp/flink/checkpoints/icdc2");
        environment.setStateBackend(fsStateBackend);
        //基于processTime不能做到完全实时
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //基于eventTime 需要设置waterMark水位线
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(4);

        //1、read data from kafka
        Properties props = new Properties();
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
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String s) throws Exception {
                    Map<String, String> map = ParamParseUtil.parse(s);
                    String w = map.getOrDefault("w", "w");
                    String c = map.getOrDefault("c", "c");
                    String m = map.getOrDefault("m", "m");
                    String wcm = String.format("%s-%s-%s", w, c, m);
                    return new Tuple2<>(wcm, 1L);
                }
            }).keyBy(0)
            .map(new MyMapFunction())
            .uid("my-map")
            .uid("my-map")
            .setParallelism(5);

        result.print();

        environment.execute("icdc state test");
    }

    static class MyMapFunction extends RichMapFunction<Tuple2<String, Long>, String> {

        private MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 配置 StateTTL(TimeToLive)
//            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(3))   // 存活时间
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 永远不返回过期的用户数据
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 每次写操作创建和更新时,修改上次访问时间戳
//                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 目前只支持 ProcessingTime
//                .build();

            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("my-map-state", String.class, Long.class);

            // 激活 StateTTL
//            mapStateDescriptor.enableTimeToLive(ttlConfig);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public String map(Tuple2<String, Long> value) throws Exception {
            String wcm = value.f0;
            Long val = mapState.get(wcm);
            if (val == null) {
                val = value.f1;
            } else {
                val += value.f1;
            }
            mapState.put(wcm, val);
            LOG.info("{}->{}", wcm, val);
            return wcm;
        }

    }

}
