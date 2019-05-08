package com.wdongyu.dataset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @program: userLog
 * @description: task 2 -- sort
 * @author: wdongyu
 * @create: 2019-04-08 09:23
 **/

public class Sort {
    public static class UserLogMapper extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(), 0, value.getLength(), "utf-8");
            String[] tokens = line.split("\\s");
            String[] t = tokens[0].split(":");
            outputKey.set(t[0]);
            outputValue.set(t[1] + ":" + tokens[1]);
            // System.out.println(outputKey.toString() + "ss" + outputValue.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static class UserLogReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            for (Text v : value) {
                // System.out.println(v.toString());
                list.add(v.toString());
            }
            Collections.sort(list, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    String[] token1 = o1.split(":");
                    String[] token2 = o2.split(":");
                    return Integer.parseInt(token2[1]) - Integer.parseInt(token1[1]);
                }
            });

            for (String l : list.subList(0, 10)) {
                context.write(key, new Text(l));
            }
        }

//        protected void cleanup(Context context) throws IOException, InterruptedException {
//
//        }
    }
}
