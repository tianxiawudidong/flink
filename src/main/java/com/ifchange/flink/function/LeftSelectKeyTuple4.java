package com.ifchange.flink.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class LeftSelectKeyTuple4 implements KeySelector<Tuple4<String, Integer, Double, Integer>, String> {
    @Override
    public String getKey(Tuple4<String, Integer, Double, Integer> tuple4) throws Exception {
        return tuple4.f0;
    }
}
