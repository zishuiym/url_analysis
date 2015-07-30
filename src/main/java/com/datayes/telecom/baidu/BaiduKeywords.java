package com.datayes.telecom.baidu;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

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

public class BaiduKeywords {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private static String baiduSearch = "www.baidu.com";
		private static String sougouSearch = "www.sogou.com";
		private static String timeFormat = "yyyyMMdd";
		private static String prefixBaidu = "s?wd=";
		private static String prefixSougou = "query=";
		private static String postfix = "&";

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String keyword = "";
			
			if (value.toString().contains(baiduSearch)) {
				
				StringTokenizer itr = new StringTokenizer(value.toString());
				int column=0;
			      while (itr.hasMoreTokens()) {
			        column++;
			        String token = itr.nextToken();
			        if(column==15)
			        {
			        	keyword = token;
			        }
			      }
				
				
				String tss[] = keyword.split(baiduSearch);
				if(tss!=null&&tss.length>1)
				{
				keyword = tss[1];
				}
				if (keyword.contains(prefixBaidu)) {
					String usefulStrs[] = keyword.split(prefixBaidu);
					if(usefulStrs!=null&&usefulStrs.length>1)
					{
						String usefulStr = usefulStrs[1];
						String ss[] = usefulStr.split(postfix);
						if(ss!=null&&ss.length>0)
						{
						keyword = ss[0];
						//System.out.println("ready to decode:"+keyword);
						try
						{
						keyword = URLDecoder.decode(keyword, "UTF-8");
						
						word.set(keyword);
						context.write(word, one);
						}
						catch(Exception e)
						{}
						}
					}
				}
			}
			/*
			 * else if(value.toString().contains(sougouSearch)) { keyword =
			 * value.toString().split(sougouSearch)[1]; keyword =
			 * keyword.split(prefixSougou)[1].split(postfix)[0]; keyword =
			 * URLDecoder.decode(keyword, "UTF-8"); String splittedStrs[] =
			 * value.toString().split(" "); dateStr =
			 * splittedStrs[splittedStrs.length-1];
			 * if(DateTimeTools.isValidDateTimeStr(dateStr, timeFormat)) {
			 * word.set("sougou_"+dateStr+"_"+keyword); context.write(word,
			 * one); } }
			 */
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Keywords Count");
		job.setJarByClass(BaiduKeywords.class);
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