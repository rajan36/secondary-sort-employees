package com.example.learning.mapper;

import com.example.learning.utils.CompositeKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SSEMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {
    Logger logger = Logger.getLogger(this.getClass());

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("Entering map method");
        try {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] values = line.trim().split("\\s+");
                String deptId = values[6];
                String lastName = values[3];
                String firstName = values[2];
                String empId = values[0];
                context.write(new CompositeKey(deptId, lastName, firstName, empId), new IntWritable(1));
            }
        } finally {
            logger.info("Exiting map method");
        }
    }
}
