
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averageprocessingtimesbytype;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Sattya
 */
public class AverageProcessingTimesByType extends Configured implements Tool{

    public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
               
        public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = key.toString();
            if(line != null) {
        	String[] cols = line.split(",");
                try {
                 //   if(!cols[17].equals("2015")) {
                       String date[] = cols[2].split(" ");
                       String firstDate = date[0];
                       String date1[] = cols[18].split(" ");
                       String lastDate = date1[0];
                                                          
                       SimpleDateFormat myFormat = new SimpleDateFormat("MM/dd/yy");
                       Date fdate = myFormat.parse(firstDate);
                       Date ldate = myFormat.parse(lastDate);
                    
                     //  System.out.println("fdate is "+ fdate.toString());
                       
                       String date2[] = fdate.toString().split(" ");
                     //  System.out.println("Day of week is "+ date2[0]);
                    
                       int days = (int)( (ldate.getTime() - fdate.getTime()) / (1000 * 60 * 60 * 24)); 
                       IntWritable daysInBetween = new IntWritable(days);
                       word.set(cols[5]);
                       output.collect(word, daysInBetween);
                 //   }
                }
                catch(Exception e){
    		  System.out.println("Bad row: "+e);
                }
            }
        }
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
             
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            int count = 0;
            while (values.hasNext()) {
                 sum += ((IntWritable)values.next()).get();
                 count++;
            }
            int avgTime = sum/count;
            output.collect(key, new IntWritable(avgTime));
                      
        } 
       
    }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, AverageProcessingTimesByType.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("AverageProcessingTimesByType");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
     //   job.set("key.value.separator.in.input.line", "");
        
        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AverageProcessingTimesByType(), args);
        System.exit(res);
    }
}

