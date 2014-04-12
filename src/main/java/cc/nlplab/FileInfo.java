package cc.nlplab;

import java.util.ArrayList;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.Text;
import lombok.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import cc.nlplab.LongArrayListW;
import cc.nlplab.TermOffsets;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
@NoArgsConstructor
public class FileInfo implements WritableComparable {
  @NonNull @Getter @Setter private String fileName;
  @NonNull @Getter @Setter private double score;
  // @NonNull @Getter @Setter private SortedMapW termOffsets;
  @NonNull @Getter @Setter private ArrayList<TermOffsets> termOffsets;

  @Override
  public void readFields(DataInput in) throws IOException {
    Text fnText = new Text();
    fnText.readFields(in);
    fileName = fnText.toString();
    score = in.readDouble();
    if(termOffsets == null)
      termOffsets = new ArrayList<TermOffsets>();
      // termOffsets = new SortedMapW();
    else
      termOffsets.clear();
    int size = in.readInt();
    for(int i = 0; i < size ; i++ ){
      TermOffsets tfs = new TermOffsets();
      tfs.readFields(in);
      termOffsets.add(tfs);
    }
    // termOffsets.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    new Text(fileName).write(out);
    out.writeDouble(score);
    // termOffsets.write(out);
    out.writeInt(termOffsets.size());
    for(TermOffsets tfs: termOffsets)
      tfs.write(out);
  }

  public int compareTo(Object object){
    FileInfo other = (FileInfo) object;
    if (this.score > other.score)
      return 1;
    else if (this.score == other.score)
      return 0;
    else return -1;
  }
  public String toString() {
    return "(fn=" + this.fileName + ", score=" + this.score + ", tm_ofs=" + this.termOffsets + ")";
  }
}


