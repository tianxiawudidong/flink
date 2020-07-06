package com.ifchange.flink.function;

import org.apache.flink.api.common.functions.MapFunction;

public class MySubStringFunction implements MapFunction<String, String> {

    @Override
    public String map(String s) throws Exception {
        return s.substring(s.indexOf("t="));
    }
}
