package com.ifchange.flink.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class RightSelectKeyTuple3 implements KeySelector<Tuple2<String, Integer>, String> {
    @Override
    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
        return tuple2.f0;
    }
}
