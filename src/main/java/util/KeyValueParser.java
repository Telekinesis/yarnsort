package util;

import org.apache.commons.math3.util.Pair;

public interface KeyValueParser<K, V>
{
    public Pair<K, V> parse(String text);
}
