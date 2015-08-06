package com.datayes.telecom.ec;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
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

/*
 * 统计每天在ec上关键词的搜索量
 */
public class EcKeywords {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		//private static String baiduSearch = "tmall.com";
		//private static String prefixBaidu[] = { "q=" };
		//private static String postfix = "&";

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String keyword = "";
			
			String txt = value.toString();
			Configuration conf = context.getConfiguration();
			String filterWord = conf.get("filterString");
			
			if (txt.contains(filterWord)) {
				
				int columnToProcess = Integer.parseInt(conf
						.get("columnToProcess"));
				String prefixList = conf.get("prefixList");
				String postfix = conf.get("postfix");
				String coding = conf.get("coding");

				StringTokenizer itr = new StringTokenizer(value.toString());
				int column = 0;
				while (itr.hasMoreTokens()) {
					column++;
					String token = itr.nextToken();
					if (column == columnToProcess) {
						keyword = token;
					}
				}
				String tss[] = keyword.split(filterWord);
				if (tss != null && tss.length > 1) {
					keyword = tss[1];
				}
				
				List<String> preList = new ArrayList<String>();
				String tmps[] = prefixList.split("_");
				for(String tmp: tmps)
				{
					preList.add(tmp);
				}
				
				for (String pre : preList) {
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
											coding);

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
		conf.set("filterString", args[5]);
		conf.set("prefixList", args[6]);
		conf.set("postfix", args[7]);
		conf.set("coding", args[8]);
		String[] otherArgs = new GenericOptionsParser(conf,
                args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Keywords Count");
		job.setJarByClass(EcKeywords.class);
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