package com.ifchange.flink.function;

import com.ifchange.flink.mysql.MysqlPool;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OnlineMarketMapFunctionState extends RichMapFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(OnlineMarketMapFunctionState.class);

    protected static RedissonClient redissonClient;

    private static final String USERNAME = "bi_user";

    private static final String PASSWORD = "7SvOaNaIJrE3oOUx";

    private static final String HOST = "192.168.9.33";

    private static final String USERNAME_2 = "databus_user";

    private static final String PASSWORD_2 = "Y,qBpk5vT@zVOwsz";

    protected MysqlPool mysqlPool3306;

    protected MysqlPool mysqlPool3307;

    protected MapState<String, String> mapState;

    public OnlineMarketMapFunctionState() {
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("my-state", String.class, String.class));
    }

    @Override
    public void open(Configuration parameters) {

        LOG.info("init mysql pool...");
        try {
            mysqlPool3306 = new MysqlPool(USERNAME, PASSWORD, HOST, 3306, "online_market");
            mysqlPool3307 = new MysqlPool(USERNAME_2, PASSWORD_2, HOST, 3307, "dzp_online_interview");
        } catch (Exception e) {
            LOG.info("init mysql error:{}", e.getMessage());
        }

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
        redissonClient.shutdown();

        mapState.clear();
    }

    @Override
    public String map(String s) throws Exception {
        return null;
    }
}
