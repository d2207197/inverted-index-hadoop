package cc.nlplab;

import cc.nlplab.TextIntWC;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;




public class TextIntArrayW extends ArrayWritable {
    public TextIntArrayW()
    {
	super(TextIntWC.class);
    }

    public TextIntArrayW(Writable[] values) {
	super(TextIntWC.class, values);
    }
    
    public String toString() {
	StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
	    {
		sb.append(s).append("::");
	    }
        return sb.toString();
    }
}

