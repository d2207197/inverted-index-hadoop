package cc.nlplab;
import org.apache.hadoop.io.Writable;


import org.apache.hadoop.io.ArrayWritable;
// import org.apache.hadoop.io.IntWritable;
import cc.nlplab.TermInfo;

public class TermInfoArray extends ArrayWritable {
  public TermInfoArray() {
    super(TermInfo.class);
  }
  public TermInfoArray(TermInfo[] values) {
    super(TermInfo.class, values);
  }

    @Override
  public String toString()
  {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(getClass().getSimpleName() + "[");
    for (Writable entry : this.get())
      {
	TermInfo termInfo = (TermInfo) entry;
	stringBuilder.append(termInfo.toString() + ", ");
      }
      // stringBuilder.append(entry.getKey().toString() + "=" + entry.getValue().toString() + ", ");

    stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
    stringBuilder.append("]");

    return stringBuilder.toString();
  }


}
