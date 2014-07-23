package sort;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SingleColumnSort extends Configured implements Tool
{
    public static class SingleColumnSortReducer extends Reducer<DoubleWritable, Text, NullWritable, Text>{
	
	@Override
	protected void reduce(DoubleWritable key, Iterable<Text> values,
	        org.apache.hadoop.mapreduce.Reducer<DoubleWritable, Text, NullWritable, Text>.Context context)
	        throws IOException, InterruptedException
	{
	    for (Text text : values)
            {
	        context.write(NullWritable.get(), new Text(key.toString() + "-" + text.toString()));
            }
	}
    }
    
    public static void main(String[] args) throws Exception
    {
	int ret = ToolRunner.run(new Configuration(), new SingleColumnSort(),
	        args);
	System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception
    {
	Job job = Job.getInstance(getConf());
	Path inputDir = new Path(args[0]);
	Path outputDir = new Path(args[1]);
	job.setJobName("SingleColumnSort");
	job.setJarByClass(SingleColumnSort.class);

	DoubleTextInputFormat.setInputPaths(job, inputDir);
	job.setInputFormatClass(DoubleTextInputFormat.class);
	FileOutputFormat.setOutputPath(job, outputDir);
	job.setReducerClass(SingleColumnSortReducer.class);
	job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(Text.class);
	
	job.setNumReduceTasks(5);
	job.setPartitionerClass(TotalOrderPartitioner.class);
	InputSampler.Sampler<DoubleWritable, Text> sampler = new InputSampler.RandomSampler<DoubleWritable, Text>(
	        0.1, 50, 10);
	Path input = DoubleTextInputFormat.getInputPaths(job)[0];
	input = input.makeQualified(input.getFileSystem(getConf()));
	Path partitionFile = new Path(input, "_partitions");
	TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
	        partitionFile);
	InputSampler.writePartitionFile(job, sampler);
	// Add to DistributedCache
	URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
	job.addCacheFile(partitionUri);
	job.createSymlink();

	return job.waitForCompletion(true) ? 0 : 1;
    }

}
