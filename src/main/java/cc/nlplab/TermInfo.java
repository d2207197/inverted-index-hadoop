package cc.nlplab;

import java.util.ArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.Text;
import lombok.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
@NoArgsConstructor
public class TermInfo implements Writable {
  @NonNull @Getter @Setter private String term;
  @NonNull @Getter @Setter private int tf;
  @NonNull @Getter @Setter private ArrayList<Long> offsets;

  @Override
  public void readFields(DataInput in) throws IOException {
    Text termText = new Text();
    termText.readFields(in);
    term = termText.toString();
    tf = in.readInt();
    int size = in.readInt();
    offsets= new ArrayList<Long>(size);
    for(int i = 0; i < size; i++){
      offsets.add(in.readLong());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    new Text(term).write(out);
    out.writeInt(tf);
    out.writeInt(offsets.size());
    for (long offset : offsets) {
      out.writeLong(offset);
    }
  }
  public String toString() {
    return "(fn=" + this.term + ", tf=" + this.tf + ", ofs=" + this.offsets + ")";
  }
}
