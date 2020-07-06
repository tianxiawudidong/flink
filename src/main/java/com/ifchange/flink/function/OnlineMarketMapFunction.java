package com.ifchange.flink.function;

import com.ifchange.flink.mysql.MysqlPool;
import com.ifchange.flink.util.RedisCli;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class OnlineMarketMapFunction extends RichMapFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(OnlineMarketMapFunction.class);

    protected static final RedisCli redisCli = new RedisCli("192.168.8.117", 6070);

    private static final String USERNAME = "bi_user";

    private static final String PASSWORD = "7SvOaNaIJrE3oOUx";

    private static final String HOST = "192.168.9.33";

    private static final String USERNAME_2 = "databus_user";

    private static final String PASSWORD_2 = "Y,qBpk5vT@zVOwsz";

    protected MysqlPool mysqlPool3306;

    protected MysqlPool mysqlPool3307;

    protected List<String> tidList = new ArrayList<>();

    public OnlineMarketMapFunction() {
        tidList.add("0");
        tidList.add("1");
        tidList.add("10");
        tidList.add("11");
        tidList.add("12");
        tidList.add("13");
        tidList.add("14");
        tidList.add("15");
        tidList.add("16");
        tidList.add("17");
        tidList.add("18");
        tidList.add("19");
        tidList.add("20");
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("init redis pool...");
        LOG.info("init mysql pool...");
        try {
            mysqlPool3306 = new MysqlPool(USERNAME, PASSWORD, HOST, 3306, "online_market");
            mysqlPool3307 = new MysqlPool(USERNAME_2, PASSWORD_2, HOST, 3307, "dzp_online_interview");
        } catch (Exception e) {
            LOG.info("init mysql error:{}", e.getMessage());
        }
    }


    @Override
    public void close() throws Exception {
        LOG.info("call mysql close...");
        LOG.info("redis return pool...");
        mysqlPool3306.close();
        mysqlPool3307.close();
        redisCli.close();
    }

    @Override
    public String map(String s) throws Exception {
        return null;
    }
}
