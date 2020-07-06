package com.ifchange.flink.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyFilterFunction implements FilterFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MyFilterFunction.class);

    @Override
    public boolean filter(String s) throws Exception {
        return StringUtils.isNoneBlank(s) && StringUtils.contains(s, "t=");
    }
}
