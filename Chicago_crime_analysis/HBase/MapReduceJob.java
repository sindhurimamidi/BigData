package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.io.IOException;


public class MapReduceJob extends Configured implements Tool {
    public static class MapClass extends TableMapper<Text, IntWritable> {

        public void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException {

            try {
                // get rowKey and convert it to string
                String inKey = new String(rowKey.get());
                byte[] year = columns.getValue(Bytes.toBytes("crimesData"), Bytes.toBytes("year"));
                String key = new String(year);
             //   System.out.println("year is "+key);
                context.write(new Text(key), new IntWritable(1));
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            try {
                int sum = 0;
                // loop through
                for (IntWritable crimes : values) {
                    Integer intCrimes = new Integer(crimes.toString());
                    sum += intCrimes;
                }

                Put insHBase = new Put(key.getBytes());
                // insert sum value to hbase
                insHBase.add(Bytes.toBytes("crimesData"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
                // write data to Hbase table
                context.write(null, insHBase);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // define scan and define column families to scan
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes("crimesData"));

        Job job = new Job(conf);

        job.setJarByClass(MapReduceJob.class);
        // define input hbase table
        TableMapReduceUtil.initTableMapperJob(
                "crimes",
                scan,
                MapClass.class,
                Text.class,
                IntWritable.class,
                job);
        // define output table
        TableMapReduceUtil.initTableReducerJob(
                "crimesSummary",
                Reduce.class,
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
       
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapReduceJob(), args);
        System.exit(res);
    }
}
