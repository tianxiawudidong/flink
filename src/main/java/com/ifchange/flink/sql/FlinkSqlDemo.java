//package com.ifchange.flink.sql;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.LocalEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.operators.MapOperator;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.sinks.CsvTableSink;
//import org.apache.flink.table.sinks.TableSink;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
//
///**
// * kafka-flink table
// *
// */
//public class FlinkSqlDemo {
//
//    private static final Logger LOG= LoggerFactory.getLogger(FlinkSqlDemo.class);
//
////    public static void main(String[] args) throws Exception {
//
////        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
////
////        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(environment);
////
////
////        DataSource<String> source = environment.readTextFile("f://score.csv");
////
////        DataSet<PlayerData> data = source.map(new MapFunction<String, PlayerData>() {
////            @Override
////            public PlayerData map(String s) throws Exception {
////                PlayerData playerData = new PlayerData();
////                //17-18,詹姆斯-哈登,72,72,35.4,8.8,1.8,0.7,30.4
////                String[] split = s.split(",");
////                playerData.setSeason(split[0]);
////                playerData.setPlayer(split[1]);
////                playerData.setPlay_num(split[2]);
////                playerData.setFirst_court(Integer.parseInt(split[3]));
////                playerData.setTime(Double.parseDouble(split[4]));
////                playerData.setAssists(Double.parseDouble(split[5]));
////                playerData.setSteals(Double.parseDouble(split[6]));
////                playerData.setBlocks(Double.parseDouble(split[7]));
////                playerData.setScores(Double.parseDouble(split[8]));
////                return playerData;
////            }
////        });
////        //3\. 注册成内存表
////        Table table = tableEnvironment.fromDataSet(data);
////        tableEnvironment.registerTable("score",table);
////
////        //4\. 编写sql 然后提交执行
////        //select player, count(season) as num from score group by player order by num desc;
////        Table queryResult = tableEnvironment.sqlQuery("select player, count(season) as num from score " +
////            "group by player order by num desc limit 3");
////
////        //5\. 结果进行打印
////        DataSet<Result> result = tableEnvironment.toDataSet(queryResult, Result.class);
////        result.print();
////
////        TableSink sink = new CsvTableSink("f://result.csv", ",");
////        String[] fieldNames = {"name", "num"};
////        TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
////        tableEnvironment.registerTableSink("result", fieldNames, fieldTypes, sink);
////        queryResult.insertInto("result");
////        environment.execute();
////
////
////    }
////
////
////    public static class PlayerData {
////        /**
////         * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
////         */
////        public String season;
////        public String player;
////        public String play_num;
////        public Integer first_court;
////        public Double time;
////        public Double assists;
////        public Double steals;
////        public Double blocks;
////        public Double scores;
////
////        public PlayerData() {
////            super();
////        }
////
////        public PlayerData(String season,
////                          String player,
////                          String play_num,
////                          Integer first_court,
////                          Double time,
////                          Double assists,
////                          Double steals,
////                          Double blocks,
////                          Double scores
////        ) {
////            this.season = season;
////            this.player = player;
////            this.play_num = play_num;
////            this.first_court = first_court;
////            this.time = time;
////            this.assists = assists;
////            this.steals = steals;
////            this.blocks = blocks;
////            this.scores = scores;
////        }
////
////        public String getSeason() {
////            return season;
////        }
////
////        public void setSeason(String season) {
////            this.season = season;
////        }
////
////        public String getPlayer() {
////            return player;
////        }
////
////        public void setPlayer(String player) {
////            this.player = player;
////        }
////
////        public String getPlay_num() {
////            return play_num;
////        }
////
////        public void setPlay_num(String play_num) {
////            this.play_num = play_num;
////        }
////
////        public Integer getFirst_court() {
////            return first_court;
////        }
////
////        public void setFirst_court(Integer first_court) {
////            this.first_court = first_court;
////        }
////
////        public Double getTime() {
////            return time;
////        }
////
////        public void setTime(Double time) {
////            this.time = time;
////        }
////
////        public Double getAssists() {
////            return assists;
////        }
////
////        public void setAssists(Double assists) {
////            this.assists = assists;
////        }
////
////        public Double getSteals() {
////            return steals;
////        }
////
////        public void setSteals(Double steals) {
////            this.steals = steals;
////        }
////
////        public Double getBlocks() {
////            return blocks;
////        }
////
////        public void setBlocks(Double blocks) {
////            this.blocks = blocks;
////        }
////
////        public Double getScores() {
////            return scores;
////        }
////
////        public void setScores(Double scores) {
////            this.scores = scores;
////        }
////    }
////
////    public static class Result {
////        public String player;
////        public Long num;
////
////        public Result() {
////            super();
////        }
////        public Result(String player, Long num) {
////            this.player = player;
////            this.num = num;
////        }
////        @Override
////        public String toString() {
////            return player + ":" + num;
////        }
////    }
//}
