package sort;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import util.KeyValueParser;

public class DoubleTextInputFormat extends CustomInputFormat<DoubleWritable, Text>{

    public DoubleTextInputFormat()
    {
	super(new KeyValueParser<DoubleWritable, Text>()
	{

	    @Override
            public Pair<DoubleWritable, Text> parse(String text)
            {
		String[] terms = text.split("\\s+");
		double key = Double.parseDouble(terms[0]);
		return new Pair<DoubleWritable, Text>(new DoubleWritable(key), new Text(text));
            }
	});
    }
}
