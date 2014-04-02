package cc.nlplab;

import java.lang.reflect.*;


// import lombok.Data;
// import lombok.AllArgsConstructor;
import lombok.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@NoArgsConstructor
@Data
public class TextPair  implements WritableComparable {
    @NonNull private Text first;
    @NonNull private Text second;

    public int compareTo(Object object) {
	TextPair ip2 = (TextPair) object;
	int cmp = getFirst().compareTo(ip2.getFirst());
	if (cmp != 0) 
	    return cmp;
	return getSecond().compareTo(ip2.getSecond()); // reverse
    }
    public void readFields(DataInput in) throws IOException {
	if (first == null)
	    first = new Text();
	if (second == null)
	    second = new Text();

	first.readFields(in);
	second.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
	first.write(out);
	second.write(out);
    }
}


