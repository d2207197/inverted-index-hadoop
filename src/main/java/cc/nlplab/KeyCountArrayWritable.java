package cc.nlplab;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import cc.nlplab.KeyCount;
import java.lang.StringBuffer;
import java.lang.String;

public class KeyCountArrayWritable extends ArrayWritable {
    public KeyCountArrayWritable() {
        super(KeyCount.class);
    }

    public KeyCountArrayWritable(Writable[] values) {
	super(KeyCount.class, values);

    }

    public String toString() {
	StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }
        return sb.toString();
    }
}
