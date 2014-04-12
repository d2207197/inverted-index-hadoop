package cc.nlplab;

import java.lang.reflect.*;


// import lombok.Data;
// import lombok.AllArgsConstructor;
import lombok.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.String;



@EqualsAndHashCode
@NoArgsConstructor
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class KeyCount  implements WritableComparable {
    @Getter @Setter @NonNull private Text key;
    @Getter @Setter @NonNull private IntWritable count;

    public int compareTo(Object object) {
        KeyCount ip2 = (KeyCount) object;
        int cmp = getKey().compareTo(ip2.getKey());
        if (cmp != 0)
            return cmp;
        return getCount().compareTo(ip2.getCount()); // reverse
    }
    public void readFields(DataInput in) throws IOException {
        if (key == null)
            key = new Text();
        if (count == null)
            count = new IntWritable();

        key.readFields(in);
        count.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        key.write(out);
        count.write(out);
    }

    public String toString() {
        return "(" + this.getKey() + ", " + this.getCount() + ")";
    }
}


