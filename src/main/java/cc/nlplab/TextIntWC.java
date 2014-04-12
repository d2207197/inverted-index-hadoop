package cc.nlplab;

import cc.nlplab.PairWC;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;




public class TextIntWC extends PairWC<Text, IntWritable> {
    public TextIntWC() {
        super(Text.class, IntWritable.class);
    }
    public TextIntWC(Text first, IntWritable second) {
        super(Text.class, IntWritable.class, first, second );
    }
}

