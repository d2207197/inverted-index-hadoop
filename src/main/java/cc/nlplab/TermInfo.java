package cc.nlplab;

import java.util.ArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.Text;
import lombok.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import cc.nlplab.LongArrayListW;


@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
@NoArgsConstructor
public class TermInfo implements Writable {
    @NonNull @Getter @Setter private String fileName;
    @NonNull @Getter @Setter private int tf;
    @NonNull @Getter @Setter private LongArrayListW offsets;

    @Override
    public void readFields(DataInput in) throws IOException {
        Text fnText = new Text();
        fnText.readFields(in);
        fileName = fnText.toString();
        tf = in.readInt();
        // int size = in.readInt();
        // offsets= new ArrayList<Long>(size);
        // for(int i = 0; i < size; i++){
        //   offsets.add(in.readLong());
        // }
        if (offsets == null)
            offsets = new LongArrayListW();
        offsets.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(fileName).write(out);
        out.writeInt(tf);
        offsets.write(out);
        // out.writeInt(offsets.size());
        // for (long offset : offsets) {
        //   out.writeLong(offset);
        // }
    }
    public String toString() {
        return "(fn=" + this.fileName + ", tf=" + this.tf + ", ofs=" + this.offsets + ")";
    }
}
