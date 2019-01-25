package com.example.learning.partitioner;

import com.example.learning.utils.CompositeKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class SSEPartitioner extends Partitioner<CompositeKey, IntWritable> {
    Logger logger = Logger.getLogger(this.getClass());

    @Override
    public int getPartition(CompositeKey compositeKey, IntWritable intWritable, int i) {
        logger.info("Entering getPartition method");
        try {
            return compositeKey.getDeptId().hashCode() % i;
        } finally {
            logger.info("Exiting getPartition method");
        }
    }
}
