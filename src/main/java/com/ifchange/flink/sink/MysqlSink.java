package com.ifchange.flink.sink;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MysqlSink extends RichSinkFunction<Tuple4<String, Integer, Double, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlSink.class);
    private static final String driver = "com.mysql.jdbc.Driver";
    private static final String USERNAME = "F0WD";
    private static final String PASSWORD = "0Io*X=5=)N2WT=FHLHp";
    private static final String URL = "jdbc:mysql://192.168.9.54:3306/bi_visual_data?useUnicode=true&characterEncoding=UTF-8&&zeroDateTimeBehavior=convertToNull";
    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private Connection connection;
    private PreparedStatement preparedStatement;
    private String sql = "insert into icdc_monitor_on_flink(from_time,end_time,wcm,call_count,fail_count,avg_time)" +
        " values(?,?,?,?,?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("call mysql sink open....");
        Class.forName(driver);
        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }

    @Override
    public void invoke(Tuple4<String, Integer, Double, Integer> tuple4, Context context) {
        try {
            if (connection == null) {
                Class.forName(driver);
                connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            }
            String wcm = tuple4.f0;
            Integer count = tuple4.f1;
            Double avgTime = tuple4.f2;
            Integer failCount = tuple4.f3;
            LocalDateTime localDateTime = LocalDateTime.now();
            LocalDateTime before = localDateTime.minusMinutes(1L);
            String from = DTF.format(before);
            String to = DTF.format(localDateTime);

            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, from);
            preparedStatement.setString(2, to);
            preparedStatement.setString(3, wcm);
            preparedStatement.setInt(4, count);
            preparedStatement.setInt(5, failCount);
            preparedStatement.setDouble(6, avgTime);
            preparedStatement.executeUpdate();

        } catch (Exception e) {
            LOG.info("call mysql sink error:{}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("call mysql sink close....");
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
