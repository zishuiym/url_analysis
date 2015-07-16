package com.datayes.hadoop.sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortByValue {

	public static class ReverseKeyValueMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		private static IntWritable newKey = new IntWritable();
		private static Text newValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			System.out.println("We are running on line:"+key.toString());
			StringTokenizer tokens = new StringTokenizer(value.toString());
			while(tokens.hasMoreTokens())
			{
				newValue.set(tokens.nextToken());
				newKey.set(Integer.parseInt(tokens.nextToken()));
			}
			System.out.println(newKey.toString()+":"+newValue.toString());
			context.write(newKey, newValue);
			
		}
	}
	
	public static class KeySectionPatitioner<K, V> extends Partitioner<K,V>
	{
		@Override
		public int getPartition(K key, V value, int numPartitions) {
			int maxValue = 10;
			int keySection = 0;
			if (numPartitions > 1 && key.hashCode() < maxValue) {
		        int sectionValue = maxValue / (numPartitions - 1);
		        int count = 0;
		        while ((key.hashCode() - sectionValue * count) > sectionValue) {
		            count++;
		        }
		        keySection = numPartitions - 1 - count;
		    }
			System.out.println("key "+key+" should be in partition: "+keySection);
			return keySection;
		}
	}
	
	public static class IntKeyDescComparator extends WritableComparator
	{
		public IntKeyDescComparator() {
		    super(IntWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
		    return -super.compare(a, b);
		}
	}
	
	public static class OutputSortedIntReducer extends Reducer<IntWritable, Text, Text, IntWritable>
	{
		private static Text res = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for(Text value : values)
			{
				res.set(value);
				context.write(res, key);
			}
		}
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sort by value");
		job.setJarByClass(SortByValue.class);
		job.setMapperClass(ReverseKeyValueMapper.class);
		job.setPartitionerClass(KeySectionPatitioner.class);
		job.setSortComparatorClass(IntKeyDescComparator.class);
		job.setReducerClass(OutputSortedIntReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
