package com.example.learning.reducer;

import com.example.learning.utils.CompositeKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SSEReducer extends Reducer<CompositeKey, IntWritable, Text, Text> {
    Logger logger = Logger.getLogger(this.getClass());

    @Override
    protected void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Entering reduce method");
        try {

            int counter = 0;
            logger.info("before counter: " + counter + " key:" + key.toString());
            for (IntWritable i : values) {
                counter++;
                logger.info("Inside counter: " + counter + " key:" + key.toString());
                context.write(new Text(key.getDeptId()), new Text(String.join("\t", new String[]{"" + counter, key.getLastName(), key.getFirstName(), key.getEmpId()})));
            }
        } finally {
            logger.info("Exiting reduce method");
        }

    }
}
