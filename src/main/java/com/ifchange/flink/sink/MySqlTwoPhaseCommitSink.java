package com.ifchange.flink.sink;

import com.ifchange.flink.util.DBConnectUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * * 自定义kafka to mysql，继承TwoPhaseCommitSinkFunction,实现两阶段提交。
 * * 功能：保证kafak to mysql 的Exactly-Once
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, Connection, Void> {

    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据入库操作
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, String objectNode, Context context) throws Exception {
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.err.println("===>date:" + date + " " + objectNode);
//        String value = objectNode.get("key").toString();
        String sql = "insert into `t_test` (`value`,`insert_time`) values (?,?)";
        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, objectNode);
        ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        //执行insert语句
        ps.executeUpdate();
        //手动制造异常
//        if(Long.parseLong(value) == 1566370543973L) System.out.println(1/0);
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://localhost:3306/cat?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "admin");
        System.err.println("start beginTransaction......."+connection);
        return connection;
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        System.err.println("start preCommit......."+connection);
    }

    /**
     * 如果invoke执行正常则提交事物
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        System.err.println("start commit......."+connection);
        DBConnectUtil.commit(connection);
    }

    @Override
    protected void recoverAndCommit(Connection connection) {
        System.err.println("start recoverAndCommit......."+connection);

    }


    @Override
    protected void recoverAndAbort(Connection connection) {
        System.err.println("start abort recoverAndAbort......."+connection);
    }
    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        System.err.println("start abort rollback......."+connection);
        DBConnectUtil.rollback(connection);
    }
}
