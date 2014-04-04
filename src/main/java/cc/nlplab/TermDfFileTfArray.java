package cc.nlplab;

import java.lang.reflect.*;


// import lombok.Data;
// import lombok.AllArgsConstructor;
import lombok.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import cc.nlplab.KeyCount;
import cc.nlplab.KeyCountArrayWritable;



@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@NoArgsConstructor
@Data
public class TermDfFileTfArray  implements Writable {
    @NonNull private KeyCount termDf;
    @NonNull private KeyCountArrayWritable fileTfArray;

    public void readFields(DataInput in) throws IOException {
	if (termDf == null)
	    termDf = new KeyCount();
	if (fileTfArray == null)
	    fileTfArray = new KeyCountArrayWritable();

	termDf.readFields(in);
	fileTfArray.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
	termDf.write(out);
	fileTfArray.write(out);
    }
}


