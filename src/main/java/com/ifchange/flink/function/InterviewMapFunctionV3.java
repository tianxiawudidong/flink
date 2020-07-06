package com.ifchange.flink.function;

import com.ifchange.flink.mysql.MysqlPool;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;


public class InterviewMapFunctionV3 extends RichMapFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(InterviewMapFunctionV3.class);

    protected static RedissonClient redissonClient;

    private static final String USERNAME = "bi_user";

    private static final String PASSWORD = "7SvOaNaIJrE3oOUx";

    private static final String HOST = "192.168.9.33";

    private static final String USERNAME_2 = "databus_user";

    private static final String PASSWORD_2 = "Y,qBpk5vT@zVOwsz";

//    private static final String USERNAME_DORIS = "root";
//
//    private static final String PASSWORD_DORIS = "root123456";
//
//    private static final String HOST_DORIS = "192.168.9.138";

    protected MysqlPool mysqlPool3306;

    protected MysqlPool mysqlPool3307;

//    protected MysqlPool mysqlPoolDoris;

    private ScheduledExecutorService scheduleExecutorService;

    public InterviewMapFunctionV3() {
    }

    @Override
    public void open(Configuration parameters) {

        scheduleExecutorService = Executors.newScheduledThreadPool(10);

        LOG.info("init mysql pool...");
        try {
            mysqlPool3306 = new MysqlPool(USERNAME, PASSWORD, HOST, 3306, "online_market");
            mysqlPool3307 = new MysqlPool(USERNAME_2, PASSWORD_2, HOST, 3307, "dzp_online_interview");
//            mysqlPoolDoris = new MysqlPool(USERNAME_DORIS, PASSWORD_DORIS, HOST_DORIS, 29030, "dzp_online_interview");
        } catch (Exception e) {
            LOG.info("init mysql error:{}", e.getMessage());
        }

        // mysql连接池 定时更新
        scheduleExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                LOG.info("-------------------------------------------------------");
                LOG.info("execute scheduler task,close mysql pool,and re init....");
                try {
                    mysqlPool3306.close();
                    mysqlPool3307.close();
//                    mysqlPoolDoris.close();
                    mysqlPool3306 = new MysqlPool(USERNAME, PASSWORD, HOST, 3306, "online_market");
                    mysqlPool3307 = new MysqlPool(USERNAME_2, PASSWORD_2, HOST, 3307, "dzp_online_interview");
//                    mysqlPoolDoris = new MysqlPool(USERNAME_DORIS, PASSWORD_DORIS, HOST_DORIS, 29030, "dzp_online_interview");
                } catch (Exception e) {
                    LOG.info("InterviewMapFunctionV3.open 定时任务报错:{}", e.getMessage());
                    e.printStackTrace();
                }
                LOG.info("-------------------------------------------------------");
            }
        }, 6, 6, TimeUnit.HOURS);


        LOG.info("init redisson client...");
        Config config = new Config();
        config.useSingleServer()
            .setAddress("redis://192.168.8.117:6070")
            .setPassword("ruixuezhaofengnian")
            .setDatabase(0);
        redissonClient = Redisson.create(config);
    }


    @Override
    public void close() throws Exception {
        LOG.info("call mysql close...");
        LOG.info("redis return pool...");
        mysqlPool3306.close();
        mysqlPool3307.close();
//        mysqlPoolDoris.close();
        redissonClient.shutdown();
    }

    @Override
    public String map(String s) throws Exception {
        return null;
    }
}
