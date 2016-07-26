package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class SparkCrimes {

    public static void main(String [] args) throws Exception {

        final long startTime = System.currentTimeMillis();


        SparkConf conf = new SparkConf().setAppName("Sattya-spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filepath = "hdfs://localhost:9000/user/Sattya/datasets/crimes.csv";
        JavaRDD<String> distFile = sc.textFile(filepath).cache();



        JavaPairRDD<String, Integer> pairs = distFile.mapToPair(s -> new Tuple2(s.split(",")[17]+ "_"+s.split(",")[8], 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        counts.coalesce(1).saveAsTextFile("hdfs://localhost:9000/user/Sattya/sparkoutputs/arrests");


    }

}
