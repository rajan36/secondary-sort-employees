package com.example.learning.driver;

import com.example.learning.mapper.SSEMapper;
import com.example.learning.partitioner.SSEPartitioner;
import com.example.learning.reducer.SSEReducer;
import com.example.learning.utils.CompositeKey;
import com.example.learning.utils.SSEGroupingComparator;
import com.example.learning.utils.SSESortingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SSEDriver extends Configured implements Tool {
    Logger logger = Logger.getLogger(this.getClass());

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SSEDriver(), args);
        System.exit(res);
    }


    public int run(String[] args) throws Exception {
        logger.info("Entering run method");
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Secondary Sorting Employee Dataset");
            job.setJarByClass(SSEDriver.class);
            job.setMapperClass(SSEMapper.class);
            job.setPartitionerClass(SSEPartitioner.class);
            job.setSortComparatorClass(SSESortingComparator.class);
            job.setGroupingComparatorClass(SSEGroupingComparator.class);
            job.setReducerClass(SSEReducer.class);
            job.setOutputKeyClass(CompositeKey.class);
            job.setOutputValueClass(IntWritable.class);

            List<String> other_args = new ArrayList<>();
            for (int i = 0; i < args.length; ++i) {
                try {
                    if ("-r".equals(args[i])) {
                        job.setNumReduceTasks(Integer.parseInt(args[++i]));
                    } else {
                        other_args.add(args[i]);
                    }
                } catch (NumberFormatException except) {
                    System.out.println("ERROR: Integer expected instead of " + args[i]);
                    return printUsage();
                } catch (ArrayIndexOutOfBoundsException except) {
                    System.out.println("ERROR: Required parameter missing from " +
                            args[i - 1]);
                    return printUsage();
                }
            }
            // Make sure there are exactly 2 parameters left.
            if (other_args.size() != 2) {
                System.out.println("ERROR: Wrong number of parameters: " +
                        other_args.size() + " instead of 2.");
                return printUsage();
            }
            FileInputFormat.setInputPaths(job, other_args.get(0));
            FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
            return (job.waitForCompletion(true) ? 0 : 1);
        } finally {
            logger.info("Exiting run method");
        }
    }

    static int printUsage() {
        System.out.println("Error: Something wrong !");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;

    }
}
