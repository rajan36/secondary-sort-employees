package com.example.learning.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class SSESortingComparator extends WritableComparator {
    Logger logger = Logger.getLogger(this.getClass());

    protected SSESortingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        logger.info("Entering compare method");
        try {
            CompositeKey firstKey = (CompositeKey) a;
            CompositeKey secondKey = (CompositeKey) b;
            if (firstKey.getDeptId().compareTo(secondKey.getDeptId()) == 0) {
                if (firstKey.getLastName().compareTo(secondKey.getLastName()) != 0) {
                    return -firstKey.getLastName().compareTo(secondKey.getLastName());
                } else if (firstKey.getFirstName().compareTo(secondKey.getFirstName()) != 0) {
                    return -firstKey.getFirstName().compareTo(secondKey.getFirstName());
                } else if (firstKey.getEmpId().compareTo(secondKey.getEmpId()) != 0) {
                    return -firstKey.getEmpId().compareTo(secondKey.getEmpId());
                } else
                    return 0;
            }
            return firstKey.getDeptId().compareTo(secondKey.getDeptId());
        } finally {
            logger.info("Exiting compare method");
        }
    }
}
