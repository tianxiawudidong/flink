package com.ifchange.flink.function;

import com.ifchange.flink.util.RedisCli;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;

public class MyMapFunction extends RichMapFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(MyMapFunction.class);

    protected static final RedisCli redisCli = new RedisCli("192.168.8.117", 6070);

    private static final String driver = "com.mysql.jdbc.Driver";

    private static final String USERNAME = "F0WD";

    private static final String PASSWORD = "0Io*X=5=)N2WT=FHLHp";

    private static final String URL = "jdbc:mysql://192.168.9.54:3306/bi_visual_data?useUnicode=true&characterEncoding=UTF-8&&zeroDateTimeBehavior=convertToNull";

    protected Jedis redis;

    protected Connection connection;

    private int index;

    public MyMapFunction(int index) {
        this.index = index;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("call MyMapFunction open...");
        LOG.info("init redis...");
        redis = redisCli.getJedis();
        redis.auth("ruixuezhaofengnian");
        redis.select(index);

        LOG.info("init mysql...");
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (Exception e) {
            LOG.info("init mysql error:{}", e.getMessage());
        }
    }


    @Override
    public void close() throws Exception {
        LOG.info("call MyMapFunction close...");
        LOG.info("redis return pool...");
        connection.close();
        redisCli.returnResource(redis);
    }

    @Override
    public String map(String s) throws Exception {
        return null;
    }
}
