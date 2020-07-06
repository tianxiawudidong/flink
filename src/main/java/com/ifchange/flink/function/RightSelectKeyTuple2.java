package com.ifchange.flink.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class RightSelectKeyTuple2 implements KeySelector<Tuple2<String, Double>, String> {
    @Override
    public String getKey(Tuple2<String, Double> tuple2) throws Exception {
        return tuple2.f0;
    }
}
