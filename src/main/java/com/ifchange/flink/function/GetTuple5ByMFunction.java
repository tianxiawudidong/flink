package com.ifchange.flink.function;

import com.ifchange.flink.util.ParamParseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GetTuple5ByMFunction implements MapFunction<String, Tuple5<String, Integer, Double, Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(GetTuple5ByMFunction.class);

    /**
     * f0->m
     * f1->请求数量
     * f2->响应时间
     * f3->失败数量
     * f4->超时数量
     */
    @Override
    public Tuple5<String, Integer, Double, Integer, Integer> map(String str) {
        String value = str.substring(str.indexOf("t="));
        LOG.info("{}", value);
        Map<String, String> map = ParamParseUtil.parse(value);
        String m = map.getOrDefault("method", "method");
        String r = map.getOrDefault("r", "0");
        String s = map.getOrDefault("s", "0");
        int callCount = 1;
        double responseTime = 0.0;
        try {
            responseTime = Double.parseDouble(r.trim());
        } catch (Exception e) {
            LOG.info("{} parse to double error", r);
        }
        int failCount = "0".equals(s) ? 1 : 0;
        int overTimeCount = responseTime >= 5000 ? 1 : 0;
        return new Tuple5<>(m, callCount, responseTime, failCount, overTimeCount);
    }
}
