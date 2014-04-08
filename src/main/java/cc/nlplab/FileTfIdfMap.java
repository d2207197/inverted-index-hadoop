package cc.nlplab;
// import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


class FileTfIdfMap extends SortedMapWritable {
  private TreeSet<WritableComparable> notFileNameSet = new TreeSet<WritableComparable>();
  private boolean notFlag= false;
  public FileTfIdfMap and(FileTfIdfMap other) {
    if (this.notFlag == false && other.notFlag == true)
      {
	System.out.println("R is not");
	this.putNotSet(other);
	System.out.println(notFileNameSet);
	  
	this.filterByNot();

	return this;
      }
    else if (this.notFlag == true && other.notFlag == false)
      {
	System.out.println("L is not");
	other.putNotSet(this);
	other.filterByNot();
	return other;
      }
    else if (this.notFlag == true && other.notFlag == true)
      {
	System.out.println("LR is not");
	this.putNotSet(other);
	return this;
      }
    else
      {
	System.out.println("NONE is not");

	ArrayList<Text> toDelete = new ArrayList<Text>();
	for(WritableComparable fileName_: this.keySet())
	  {
	    Text fileName = (Text)fileName_;
	  if(! other.containsKey(fileName))
	    toDelete.add(fileName);
	  else
	    this.put(fileName, new DoubleWritable (((DoubleWritable)this.get(fileName)).get() + ((DoubleWritable)other.get(fileName)).get()));
	    
	  }

	for(Text fileName: toDelete)
	  this.remove(fileName);

	return this;
      }
  }
  
  public FileTfIdfMap or(FileTfIdfMap other) {
    for(WritableComparable fileName_: other.keySet()){
      Text fileName = (Text)fileName_;
      Double tfIdf = ((DoubleWritable)this.get(fileName)).get();
      if( tfIdf == null ||  tfIdf < ((DoubleWritable)other.get(fileName)).get())
	this.put(fileName, other.get(fileName));
    }
    this.filterByNot();
    return this;
  }
  private void filterByNot() {
    for(WritableComparable fileName_: this.notFileNameSet)
      {
	Text fileName = (Text)fileName_;
	this.remove(fileName);
      }
  }
  private void putNotSet(FileTfIdfMap other) {
    this.notFileNameSet.addAll(other.notFileNameSet);
  }
  public FileTfIdfMap not() {
    System.out.println("not: " + this);
    notFileNameSet.addAll(this.keySet());
    System.out.println("not Set: " + notFileNameSet);
    notFlag = true;
    return this;
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
