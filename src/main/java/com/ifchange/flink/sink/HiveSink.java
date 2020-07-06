package com.ifchange.flink.sink;


import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveSink extends RichSinkFunction<Tuple7<String, String, String, Integer, Double, Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSink.class);

    private StreamTableEnvironment tableEnv;

    public HiveSink(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void invoke(Tuple7<String, String, String, Integer, Double, Integer, Integer> value, Context context) {
        try {
            String from = value.f0;
            String to = value.f1;
            String wcm = value.f2;
            Integer totalCount = value.f3;
            Double avgTime = value.f4;
            Integer failCount = value.f5;
            Integer overTimeCount = value.f6;
            String sql = "insert into icdc_flink_hive_test(from_time,end_time,wcm,total_count,avg_time,fail_count,over_time_count)" +
                "values('" + from + "','" + to + "','" + wcm + "'," + totalCount + "," + avgTime + "," + failCount + "," + overTimeCount + ")";
            LOG.info(sql);
            tableEnv.execute(sql);
        } catch (Exception e) {
            LOG.info("call mysql sink error:{}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
    }
}
