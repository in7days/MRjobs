package com.pavan.mapreduce;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
public class TestUDF extends UDF {
    private static final int MATCH = 1;
    private static final int NOT_MATCH = 0;
    public int evaluate(String aids, String bids, int ignoreNullFlag) {
        if (StringUtils.isBlank(aids) && StringUtils.isBlank(bids)) {
            if (ignoreNullFlag == 1) {
                return MATCH;
            } else {
                return NOT_MATCH;
            }
        } else if (StringUtils.isBlank(aids) && !StringUtils.isBlank(bids)) {
            return NOT_MATCH;
        } else if (!StringUtils.isBlank(aids) && StringUtils.isBlank(bids)) {
            return NOT_MATCH;
        } else {
            String[] aidArray = aids.split(",");
            String[] bidArray = bids.split(",");
            for (String aid : aidArray) {
                boolean exist = false;
                for (String bid : bidArray) {
                    if (aid.equals(bid)) {
                        exist = true;
                    }
                }
                if (!exist) {
                    return NOT_MATCH;
                }
            }
            return MATCH;
        }
    }
}