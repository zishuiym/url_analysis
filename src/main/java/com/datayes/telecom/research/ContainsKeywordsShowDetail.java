package com.datayes.telecom.research;

import java.io.IOException;
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 统计每天在百度上关键词的搜索量
 */
public class ContainsKeywordsShowDetail {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text line = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String keyword = conf.get("keyword");
			String encodedKeyword = URLEncoder.encode(keyword,"UTF-8");
			String txt = value.toString();
			if(txt.contains(keyword)||txt.contains(encodedKeyword))
			{
				line.set(txt);
				context.write(word, line);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("keyword", args[3]);
		String[] otherArgs = new GenericOptionsParser(conf,
                args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Keywords output");
		job.setJarByClass(ContainsKeywordsShowDetail.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}