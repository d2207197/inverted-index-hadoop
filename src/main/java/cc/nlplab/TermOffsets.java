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
public class TermOffsets implements Writable {
  @NonNull @Getter @Setter private String term;
  @NonNull @Getter @Setter private LongArrayListW offsets;

  @Override
  public void readFields(DataInput in) throws IOException {
    Text tmText = new Text();
    tmText.readFields(in);
    this.term = tmText.toString();
    if (offsets == null)
      offsets = new LongArrayListW();
    offsets.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    new Text(this.term).write(out);
    offsets.write(out);
  }

  // public int compareTo(TermOffsets other){
  //   if (this.score > other.score)
  //     return 1;
  //   else if (this.score == other.score)
  //     return 0;
  //   else return -1;
  // }
  public String toString() {
    return "(tm=" + this.term + ", ofs=" + this.offsets + ")";
  }
}


