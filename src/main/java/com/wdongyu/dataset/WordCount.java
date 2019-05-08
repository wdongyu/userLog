package com.wdongyu.dataset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @program: userLog
 * @description: Task 1 -- word count
 * @author: wdongyu
 * @create: 2019-04-08 09:03
 **/

public class WordCount {
    public static class UserLogMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(), 0, value.getLength(), "utf-8");
            String[] tokens = line.split(",");

            if (!tokens[7].equals("2"))
                return;

            outputKey.set(tokens[tokens.length-1] + ":" + tokens[1]);
            context.write(outputKey, one);
        }
    }

    public static class UserLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : value) {
                sum += i.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
}
