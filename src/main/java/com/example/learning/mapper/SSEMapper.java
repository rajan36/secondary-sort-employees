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

    public enum Dept {
        D01, D02, D03, D04, D05, D06, D07, D08, D09, D10;
    }

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
                if (deptId.endsWith("04")) {
                    context.getCounter("Dept",deptId).increment(1L);
                }
                else if(deptId.endsWith("06")) {
                    context.getCounter(Dept.D06).increment(1L);
                }
            }
        } finally {
            logger.info("Exiting map method");
        }
    }
}
