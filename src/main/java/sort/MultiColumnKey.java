package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class MultiColumnKey implements WritableComparable<MultiColumnKey>
{
    private DoubleWritable firstTerm  = new DoubleWritable();
    private DoubleWritable secondTerm = new DoubleWritable();
    
    public MultiColumnKey()
    {
    }

    public MultiColumnKey(double first, double second)
    {
	firstTerm.set(first);
	secondTerm.set(second);
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
	firstTerm.readFields(input);
	secondTerm.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException
    {
	firstTerm.write(output);
	secondTerm.write(output);
    }

    @Override
    public int compareTo(MultiColumnKey o)
    {
	int cmp = firstTerm.compareTo(o.firstTerm);
	if (cmp != 0) return cmp;
	return secondTerm.compareTo(o.secondTerm);
    }

    @Override
    public String toString()
    {
	return firstTerm.toString() + "==" + secondTerm.toString();
    }

}
