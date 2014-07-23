package sort;

import java.io.IOException;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import util.KeyValueParser;

public class CustomInputFormat<K, V> extends FileInputFormat<K, V>
{
    private final KeyValueParser<K, V> parser;
    
    public CustomInputFormat(KeyValueParser<K, V> parser)
    {
	this.parser = parser;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit arg0,
            TaskAttemptContext arg1) throws IOException, InterruptedException
    {
	return new RecordReader<K, V>()
	{
	    private final LineRecordReader innerReader = new LineRecordReader();
	    private K         currentKey;
	    private V                   currentValue;

	    @Override
	    public void close() throws IOException
	    {
		innerReader.close();
	    }

	    @Override
	    public K getCurrentKey() throws IOException,
		    InterruptedException
	    {
		return currentKey;
	    }

	    @Override
	    public V getCurrentValue() throws IOException,
		    InterruptedException
	    {
		return currentValue;
	    }

	    @Override
	    public float getProgress() throws IOException, InterruptedException
	    {
		return innerReader.getProgress();
	    }

	    @Override
	    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
		    throws IOException, InterruptedException
	    {
		innerReader.initialize(arg0, arg1);
	    }

	    @Override
	    public boolean nextKeyValue() throws IOException,
		    InterruptedException
	    {
		String line;
		if (innerReader.nextKeyValue())
		{
		    Text innerValue = innerReader.getCurrentValue();
		    line = innerValue.toString();
		}
		else
		{
		    return false;
		}
		if (line == null) return false;
		Pair<K, V> serialized = parser.parse(line);

		currentKey = serialized.getFirst();
		currentValue = serialized.getSecond();
		return true;
	    }
	};
    }

}
