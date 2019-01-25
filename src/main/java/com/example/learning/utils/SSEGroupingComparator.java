package com.example.learning.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class SSEGroupingComparator extends WritableComparator {
    Logger logger = Logger.getLogger(this.getClass());

    protected SSEGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        logger.info("Entering compare method");
        try {
            CompositeKey firstKey = (CompositeKey) a;
            CompositeKey secondKey = (CompositeKey) b;

            return firstKey.getDeptId().compareTo(secondKey.getDeptId());
        } finally {
            logger.info("Exiting compare method");
        }
    }
}
