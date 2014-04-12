package cc.nlplab;

import cc.nlplab.PairWC;
import org.apache.hadoop.io.Text;


public class TextPairWC extends PairWC<Text, Text> {
    public TextPairWC() {
        super(Text.class, Text.class);
    }
    public TextPairWC(Text first, Text second) {
        super(Text.class, Text.class, first, second );
    }
}

