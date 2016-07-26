
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crimesbyblockbymonth;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class CrimesByBlockByMonth extends Configured implements Tool{

    public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String blockName = "100XX W OHARE ST";
        
        public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = key.toString();
            if(line != null) {
        	String[] cols = line.split(",");
                try {
                    if(cols[3].equals(blockName)) {
                        String date = cols[2];
                        Pattern p = Pattern.compile("(\\d*)/(\\d*)/(\\d*)\\s*(\\d*):(\\d*)");
                        Matcher m = p.matcher(date);
                        if(m.find()) {
                          String month = getMonth(m.group(1));
                          String year= "20"+m.group(3);
                          word.set(year+"_"+month);
                          output.collect(word, one);
                        }
                       
                    }
                }
                catch(Exception e){
                    System.out.println("Bad row: "+e);
                }
            }
        }
        
        public String getMonth(String month) {
            
            switch (month) {
            case "1" :  return "Jan";
            case "01":  return "Jan";
            case "2" :  return "Feb";
            case "02":  return "Feb";        
            case "3" :  return "Mar";
            case "03":  return "Mar";         
            case "4" :  return "Apr";
            case "04":  return "Apr";      
            case "5" :  return "May";
            case "05":  return "May";       
            case "6" :  return "Jun";
            case "06":  return "Jun";        
            case "7" :  return "Jul";
            case "07":  return "Jul";       
            case "8" :  return "Aug";
            case "08":  return "Aug";        
            case "9" :  return "Sep";
            case "09":  return "Sep";         
            case "10":  return "Oct";
            case "11":  return "Nov";
            case "12":  return "Dec";
            default  : return  "Invalid month";
            }            
        }
        
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
             
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                 sum += ((IntWritable)values.next()).get();
            }
            output.collect(key, new IntWritable(sum));
           
        } 
       
    }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, CrimesByBlockByMonth.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("CrimesByBlockByMonth");
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
        int res = ToolRunner.run(new Configuration(), new CrimesByBlockByMonth(), args);
        System.exit(res);
    }
}

