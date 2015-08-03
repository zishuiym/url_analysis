package com.datayes.telecom.research;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 把和百度相关的数据dump下来
 */
public class GenSomethingRelatedItems {
	
	private static Text mapperKey = new Text();
	private static Text mapperValue = new Text();

	public static class SomethingRelatedItemMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// System.out.println("In file: " + fileName);
			Configuration conf = context.getConfiguration();
			String keyword = conf.get("something");
			
			if (value.toString().contains(keyword)) {
				
				mapperKey.set(value.toString());
				mapperValue.set("");
				context.write(mapperKey, mapperValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("something", args[2]);
		Job job = Job.getInstance(conf, "something related search");
		job.setJarByClass(GenSomethingRelatedItems.class);
		job.setMapperClass(SomethingRelatedItemMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
