package com.datayes.hadoop.algorithm;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datayes.common.InvalidInputMatrixException;

public class MatrixMultiplier {

	public static final int MATRIX_I = 4;
	public static final int MATRIX_J = 3;
	public static final int MATRIX_K = 2;

	public static class SplitMatrixMapper extends
			Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text mapperKey = new Text();
		private Text mapperValue = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());

			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			System.out.println("In file: " + fileName);

			if (fileName.contains("matrixA")) {
				int hang = 0;
				int lie = 0;
				String v = "";
				while (tokenizer.hasMoreTokens()) {
					hang = Integer.parseInt(tokenizer.nextToken());
					lie = Integer.parseInt(tokenizer.nextToken());
					v = tokenizer.nextToken();
				}
				for (int i = 1; i <= MATRIX_K; i++) {
					if (hang > 0 && lie > 0) {
						mapperKey.set(hang + "_" + i);
						mapperValue.set("A_" + lie + "_" + v);
						context.write(mapperKey, mapperValue);
					} else {
						try {
							throw new InvalidInputMatrixException(
									"valid input matrix!");
						} catch (InvalidInputMatrixException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}

			if (fileName.contains("matrixB")) {
				int hang = 0;
				int lie = 0;
				String v = "";
				while (tokenizer.hasMoreTokens()) {
					hang = Integer.parseInt(tokenizer.nextToken());
					lie = Integer.parseInt(tokenizer.nextToken());
					v = tokenizer.nextToken();
				}
				for (int i = 1; i <= MATRIX_I; i++) {
					if (hang > 0 && lie > 0) {
						mapperKey.set(i + "_" + lie);
						mapperValue.set("B_" + hang + "_" + v);
						context.write(mapperKey, mapperValue);
					} else {
						try {
							throw new InvalidInputMatrixException(
									"valid input matrix!");
						} catch (InvalidInputMatrixException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	public static class MultiplyReducer extends
			Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double[] arrayA = new double[MATRIX_J];
			double[] arrayB = new double[MATRIX_J];
			for (Text val : values) {
				String tmpStr[] = val.toString().split("_");
				if(tmpStr[0].equals("A"))
				{
					arrayA[Integer.parseInt(tmpStr[1])-1]=Double.parseDouble(tmpStr[2]);
				}
				if(tmpStr[0].equals("B"))
				{
					arrayB[Integer.parseInt(tmpStr[1])-1]=Double.parseDouble(tmpStr[2]);
				}
			}
			double res = 0.0;
			for(int i=0;i<arrayA.length;i++)
			{
				res+=arrayA[i]*arrayB[i];
			}
			result.set(res);
			for(int i=0;i<arrayA.length;i++)
			{
				System.out.println("arrayA"+i+":"+arrayA[i]);
				System.out.println("arrayB"+i+":"+arrayB[i]);
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "matrix multiplier");
		job.setJarByClass(MatrixMultiplier.class);
		job.setMapperClass(SplitMatrixMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(MultiplyReducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
