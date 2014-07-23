package sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestInputFormat extends Configured implements Tool
{

    public static class PrintOnlyMapper extends
	    Mapper<MultiColumnKey, Text, MultiColumnKey, Text>
    {
	@Override
	protected void map(
		MultiColumnKey key,
	        Text value,
	        org.apache.hadoop.mapreduce.Mapper<MultiColumnKey, Text, MultiColumnKey, Text>.Context context)
	        throws IOException, InterruptedException
	{
	    context.write(key, value);
	}
    }

    public static void main(String[] args) throws Exception
    {
	int ret = ToolRunner.run(new Configuration(), new TestInputFormat(),
	        args);
	System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception
    {
	Job job = Job.getInstance(getConf());
	Path inputDir = new Path(args[0]);
	Path outputDir = new Path(args[1]);
	job.setJobName("TestInputFormat");
	job.setJarByClass(TestInputFormat.class);
	job.setOutputKeyClass(MultiColumnKey.class);
	job.setOutputValueClass(Text.class);
	MultiColumnInputFormat.setInputPaths(job, inputDir);
	job.setInputFormatClass(MultiColumnInputFormat.class);
	FileOutputFormat.setOutputPath(job, outputDir);
	job.setMapperClass(PrintOnlyMapper.class);
	job.setNumReduceTasks(0);
	return job.waitForCompletion(true) ? 0 : 1;
    }

}
