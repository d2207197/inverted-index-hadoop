package cc.nlplab;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

import java.lang.StringBuilder;

import java.util.Map;


public class SortedMapW extends SortedMapWritable
{

  public SortedMapW(){
    super();
  }


  public SortedMapW(SortedMapW other){
    super(other);
  }

  @Override
  public String toString()
  {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(getClass().getSimpleName() + "{");
    for (Map.Entry<WritableComparable, Writable> entry : this.entrySet())
      stringBuilder.append(entry.getKey().toString() + "=" + entry.getValue().toString() + ", ");

    stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
    stringBuilder.append("}");

    return stringBuilder.toString();
  }
}
