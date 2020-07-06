package com.ifchange.flink.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class MyMapV2Function extends RichMapFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(MyMapV2Function.class);

    protected Jedis redis;

    private int index;

    public MyMapV2Function(int index) {
        this.index = index;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("call MyMapFunction open...");
        LOG.info("init redis...");
        redis = new Jedis("192.168.8.117", 6070, 10000, 5000);
        redis.auth("ruixuezhaofengnian");
        redis.select(index);
    }


    @Override
    public void close() throws Exception {
        LOG.info("call MyMapFunction close...");
        redis.close();
    }

    @Override
    public String map(String s) throws Exception {
        return null;
    }
}
