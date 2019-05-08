package com.wdongyu.dataset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @program: userLog
 * @description: user_log dataset in Spark Job
 * @author: wdongyu
 * @create: 2019-04-08 20:08
 **/

public class SparkUserLog {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                .appName("UserLog")
                                .getOrCreate();

        JavaRDD<String> rdd = spark.sparkContext().textFile(args[0], 1).toJavaRDD();
        JavaPairRDD<String, Integer> result = rdd

                .filter( s -> { String[] tokens = s.split(",");
                                return tokens[7].equals("2");})

                .mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String key = tokens[tokens.length - 1] + tokens[2];
                        return new Tuple2<String, Integer>(key, 1);
                    }
                })

                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                });

        result.saveAsTextFile(args[1]);

    }
}
