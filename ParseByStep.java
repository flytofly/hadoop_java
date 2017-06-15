package cn.mmdata.work;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import cn.mmdata.util.UrlUtil;
public class ParseByStep {
	
	static class stepMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] datas = value.toString().split("\\|");
			if(datas.length >= 26){
				String phone = datas[0];
				String hostdemo = datas[25];
				String topmain = "";
				String submain = "";
				String host = "";
				if(hostdemo.startsWith("http://")){
					topmain = UrlUtil.getTopDomain(hostdemo);
					submain = UrlUtil.getSubDomain(hostdemo);
					if(StringUtils.isNotEmpty(topmain)){
						String host_ = hostdemo.substring(7, hostdemo.length());
						int indexOf = host_.indexOf("/");
						if(indexOf > 0){
							host = host_.substring(indexOf+1);
							context.write(new Text(topmain+"_1_"+submain), new Text(topmain+";"+host+";"+phone));
						}else{
							context.write(new Text(topmain+"_1_"+submain), new Text(topmain+";"+""+";"+phone));
						}
					}
				}else{
					topmain = UrlUtil.getTopDomain("http://"+hostdemo);
					if(StringUtils.isNotEmpty(topmain)){
						submain = UrlUtil.getSubDomain("http://"+hostdemo);
						int indexOf = hostdemo.indexOf("/");
						if(indexOf > 0){
							host = hostdemo.substring(indexOf+1);
							context.write(new Text(topmain+"_1_"+submain), new Text(topmain+";"+host+";"+phone));
						}else{
							context.write(new Text(topmain+"_1_"+submain), new Text(topmain+";"+""+";"+phone));
						}
					}
				}
			}
		}
	}
	
	
	static class stepReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			  String topmain = "";
			  StringBuffer hostBUffer = new StringBuffer();
			  HashSet<String> set = new HashSet<String>();
			  ArrayList<String> list = new ArrayList<String>();
			  String phone = "";
			  String path = "";
			  String[] split = null;
			  for(Text t:values){
				  	split = t.toString().split(";");
				  	if(split.length == 3){
				  		topmain = split[0];
				  		phone = split[2];
				  		path = split[1];
				  		if(StringUtils.isNotEmpty(path)){
				  			hostBUffer.append(path).append("_").append(phone).append(",");
				  		}
				  		set.add(phone);
				  		list.add(phone);
				  	}
			  }
			  String hostS = "";
			  if(hostBUffer.toString().length() > 0 && hostBUffer.toString().endsWith(",")){
				  hostS = hostBUffer.substring(0, hostBUffer.toString().length()-1);
			  }
			  context.write(new Text(key+"|"+hostS+"|"+set.size()+"|"+list.size()),new Text(""));
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job =new Job(conf, "ParseByStep");//设置任务名
		job.setJarByClass(ParseByStep.class);//指定class
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(stepMapper.class);//调用上面Map类作为Map任务代码
		job.setReducerClass(stepReducer.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
