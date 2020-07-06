package com.ifchange.flink.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class LeftSelectKeyTuple3 implements KeySelector<Tuple3<String, Integer, Double>, String> {
    @Override
    public String getKey(Tuple3<String, Integer, Double> tuple3) throws Exception {
        return tuple3.f0;
    }
}
