package com.ifchange.flink.util;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ParamParseUtil implements Serializable {


    public static Map<String, String> parse(String value) {
        Map<String, String> result = new HashMap<>();
        if (StringUtils.isNoneBlank(value)) {
            String[] split = value.split("&");
            if (split.length > 0) {
                for (String text : split) {
                    String[] split1 = text.split("=");
                    if (split1.length == 2) {
                        String key = split1[0];
                        String values = split1[1];
                        result.put(key, values);
                    }
                    if (split1.length == 1) {
                        String key = split1[0];
                        String values = split1[0];
                        result.put(key, values);
                    }
                }
            }
        }
        return result;
    }
}
