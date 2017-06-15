package cn.mmdata.work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.mmdata.util.HBaseUtil;

public class HbaseSql extends Configured implements Tool{
	/**文件在out1
	 * 导入hbase   01zhuanche.com_1_api.01zhuanche.com
	 * @param args
	 */
	 public static void main(String[] args)
	   {
	     try
	     {
	       int response = ToolRunner.run(HBaseConfiguration.create(), 
	         new HbaseSql(), args);
	       if (response == 0) {
	         System.out.println("Job is successfully completed...");
	       } else {
	         System.out.println("Job failed...");
	       }
	     }
	     catch (Exception exception)
	     {
	       exception.printStackTrace();
	     }
	   }
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String outputPath = args[1];
	    String table = args[2];	
	    Configuration configuration = HBaseUtil.configuration;
	     configuration.set("hbase.table.name", table);
	     configuration.set("COLUMN_FAMILY", "info");
	     configuration.set("QUALIFIER1", "count");
	     Job job = Job.getInstance(configuration, "Bulk Loading HBase Table::" + table);
	     job.setJarByClass(HbaseSql.class);
	     job.setInputFormatClass(TextInputFormat.class);
	     job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	     job.setMapOutputValueClass(Put.class);
	     job.setMapperClass(BulkLoadMapper.class);
	     
	     job.setNumReduceTasks(10);
	     FileInputFormat.addInputPaths(job, args[0]);
	     FileSystem fs = FileSystem.get(configuration);
	     Path output = new Path(outputPath);
	     if (fs.exists(output)) {
	       fs.delete(output, true);
	     }
	     FileOutputFormat.setOutputPath(job, output);
	     Connection connection = ConnectionFactory.createConnection(configuration);
	     TableName tableName = TableName.valueOf(table);
	     HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
	     job.waitForCompletion(true);
	     if (job.isSuccessful())
	     {
	       HBaseUtil.doBulkLoad(outputPath, table);
	       return 0;
	     }
	     return 1;
	}
	
	public static class BulkLoadMapper
    extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
  {
    private String columnFamily;
    private String qualifier1;
    
    public void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
    {
      Configuration configuration = context.getConfiguration();
      configuration.get("hbase.table.name");
      this.columnFamily = configuration.get("COLUMN_FAMILY");
      this.qualifier1 = configuration.get("QUALIFIER1");
    }
    
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
    {
      try
      {
        String[] values = value.toString().split("\\|");
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(values[0].getBytes());
        Put put = new Put(Bytes.toBytes(values[0]));
        String vals = values[2]+"_"+values[3]+"_"+values[4];
        put.addColumn(Bytes.toBytes(this.columnFamily), Bytes.toBytes(this.qualifier1), Bytes.toBytes(vals));
        context.write(rowKey,put);
      }
      catch (Exception exception)
      {
        exception.printStackTrace();
      }
    }
  }
}
