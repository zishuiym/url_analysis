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
import org.apache.hadoop.util.GenericOptionsParser;

import com.datayes.tools.DateTimeTools;

/*
 * 统计每天在百度上关键词的搜索量
 */
public class BaiduKeywords {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private static String baiduSearch = "baidu.com";
		private static String prefixBaidu[] = { "wd=", "word=", "query=",};
		private static String postfix = "&";

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String keyword = "";

			if (value.toString().contains(baiduSearch)) {

				Configuration conf = context.getConfiguration();
				int columnToProcess = Integer.parseInt(conf
						.get("columnToProcess"));

				StringTokenizer itr = new StringTokenizer(value.toString());
				int column = 0;
				while (itr.hasMoreTokens()) {
					column++;
					String token = itr.nextToken();
					if (column == columnToProcess) {
						keyword = token;
					}
				}
				String tss[] = keyword.split(baiduSearch);
				if (tss != null && tss.length > 1) {
					keyword = tss[1];
				}
				for (String pre : prefixBaidu) {
					if (keyword.contains(pre)) {
						String usefulStrs[] = keyword.split(pre);
						if (usefulStrs != null && usefulStrs.length > 1) {
							String usefulStr = usefulStrs[1];
							String ss[] = usefulStr.split(postfix);
							if (ss != null && ss.length > 0) {
								keyword = ss[0];
								// System.out.println("ready to decode:"+keyword);
								try {
									keyword = URLDecoder.decode(keyword,
											"UTF-8");

									word.set(keyword);
									context.write(word, one);
								} catch (Exception e) {
									word.set(keyword);
									context.write(word, one);
								}
							}
						}
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
			Configuration conf = context.getConfiguration();
			int min_filter_num = Integer.parseInt(conf.get("min_filter_num"));
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			if(sum>min_filter_num)
			{
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("columnToProcess", args[3]);
		conf.set("min_filter_num", args[4]);
		String[] otherArgs = new GenericOptionsParser(conf,
                args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Keywords Count");
		job.setJarByClass(BaiduKeywords.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}