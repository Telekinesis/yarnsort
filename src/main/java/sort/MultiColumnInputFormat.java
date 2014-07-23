package sort;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;

import util.KeyValueParser;

public class MultiColumnInputFormat extends
        CustomInputFormat<MultiColumnKey, Text>
{
    public MultiColumnInputFormat()
    {
	super(new KeyValueParser<MultiColumnKey, Text>()
	{

	    @Override
	    public Pair<MultiColumnKey, Text> parse(String text)
	    {
		String[] terms = text.split("\\s+");
		MultiColumnKey key = new MultiColumnKey(
		        Double.parseDouble(terms[1]),
		        Double.parseDouble(terms[2]));
		Text value = new Text(text);
		return new Pair<MultiColumnKey, Text>(key, value);
	    }
	});
    }
}
