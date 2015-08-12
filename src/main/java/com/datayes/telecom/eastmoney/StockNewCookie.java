package com.datayes.telecom.eastmoney;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 统计每天在东方财富上个股页面的搜索量
 */
public class StockNewCookie {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text line = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String keyword = conf.get("keyword");
			String encodedKeyword = URLEncoder.encode(keyword, "UTF-8");
			String txt = value.toString();
			if (txt.contains(keyword) || txt.contains(encodedKeyword)) {
				String urlListStr = conf.get("urlList");
				List<String> urlList = getUrlListByStr(urlListStr);
				for(String str: urlList)
				{
					if(txt.contains(str))
					{
						line.set(txt);
						context.write(word, line);
					}
				}
				
			}
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
		conf.set("keyword", args[3]);
		String urlListFilePath = args[4];
		String[] otherArgs = new GenericOptionsParser(conf,
                args).getRemainingArgs();
		conf.set("urlList", genUrlListStr(urlListFilePath));
		
		Job job = Job.getInstance(conf, "Keywords output");
		job.setJarByClass(StockNewCookie.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}