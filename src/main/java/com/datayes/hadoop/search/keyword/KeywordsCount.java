package com.datayes.hadoop.search.keyword;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datayes.tools.DateTimeTools;

public class KeywordsCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    private static String baiduSearch = "www.baidu.com";
    private static String sougouSearch = "www.sogou.com";
    private static String timeFormat = "yyyyMMdd";
    private static String prefixBaidu = "s?wd=";
    private static String prefixSougou = "query=";
    private static String postfix = "&";
    
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String keyword = "";
      String dateStr = "";
      if(value.toString().contains(baiduSearch))
      {
    	  keyword = value.toString().split(baiduSearch)[1];
    	  keyword = keyword.split(prefixBaidu)[1].split(postfix)[0];
    	  keyword = URLDecoder.decode(keyword, "UTF-8"); 
    	  String splittedStrs[] = value.toString().split(" ");
    	  dateStr = splittedStrs[splittedStrs.length-1];
    	  if(DateTimeTools.isValidDateTimeStr(dateStr, timeFormat))
    	  {
    		  word.set("baidu_"+dateStr+"_"+keyword);
    		  context.write(word, one);
    	  }
      }
      else if(value.toString().contains(sougouSearch))
      {
    	  keyword = value.toString().split(sougouSearch)[1];
    	  keyword = keyword.split(prefixSougou)[1].split(postfix)[0];
    	  keyword = URLDecoder.decode(keyword, "UTF-8"); 
    	  String splittedStrs[] = value.toString().split(" ");
    	  dateStr = splittedStrs[splittedStrs.length-1];
    	  if(DateTimeTools.isValidDateTimeStr(dateStr, timeFormat))
    	  {
    		  word.set("sougou_"+dateStr+"_"+keyword);
    		  context.write(word, one);
    	  }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Keywords Count");
    job.setJarByClass(KeywordsCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}