package com.datayes.telecom.eastmoney;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 统计每天在东方财富上个股页面的搜索量
 */
public class StockFrequency {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String txt = value.toString();
			Configuration conf = context.getConfiguration();
			String filterWord = conf.get("filterWord");
			
			if(txt.contains(filterWord))
			{
				String urlListStr = conf.get("urlList");
				List<String> urlList = getUrlListByStr(urlListStr);
				for(String str : urlList)
				{
					//String encodedstr = URLEncoder.encode(str,"UTF-8");
					
					//if(txt.contains(str)||txt.contains(encodedstr))
					if(txt.contains(str))
					{
						word.set(str);
						context.write(word, one);
					}
				}
			}
		}

	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static String genUrlListStr(String urlListFilePath)
	{
		StringBuffer sb= new StringBuffer("");
		try
		{
	        FileReader reader = new FileReader(urlListFilePath);
	        BufferedReader br = new BufferedReader(reader);
	        String str = null;
	        while((str = br.readLine()) != null) {
	              sb.append(str+"\n");
	        }
	        br.close();
	        reader.close();
			}
		catch(IOException e)
		{
			e.printStackTrace();
		}
        String urlListStr=sb.toString();
        //System.out.println(urlListStr);
		return urlListStr;
	}
	
	public static List<String> getUrlListByStr(String urlListStr)
	{
		List<String> urlList = new ArrayList<String>();
		String tmps[] = urlListStr.split("\n");
		for(String tmp: tmps)
		{
			urlList.add(tmp);
		}
		//System.out.println(urlList);
		return urlList;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String urlListFilePath = args[3];
		String[] otherArgs = new GenericOptionsParser(conf,
                args).getRemainingArgs();
		conf.set("filterWord", args[4]);
		conf.set("urlList", genUrlListStr(urlListFilePath));
		Job job = Job.getInstance(conf, "Keywords Count");
		job.setJarByClass(StockFrequency.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		//getUrlListByStr(genUrlListStr("lib/eastmoney/urls.txt"));
	}
}