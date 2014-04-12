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

    // public TermInfoArray and(TermInfoArray other) {
    //   Writable [] ours = this.get();
    //   Writable [] theirs = other.get();
    //   ArrayList<TermInfo> result = new ArrayList<TermInfo>();

    //   oi = 0;
    //   ti = 0;
    //   while(oi < ours.length && ti < theirs.length){
    //     TermInfo our = (TermInfo)ours[oi];
    //     TermInfo their = (TermInfo)theirs[ti];
    //     if (our.getFileName() == their.getFileName()){
    // double sum_tfIdf = our.getTfIdf() + their.getTfIdf();
    // result.add(new TermInfo(their.getFileName(), 0));

    //     }
    //   }

    // }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getClass().getSimpleName() + "[");
        for (Writable entry : this.get()) {
            TermInfo termInfo = (TermInfo) entry;
            stringBuilder.append(termInfo.toString() + ", ");
        }
        // stringBuilder.append(entry.getKey().toString() + "=" + entry.getValue().toString() + ", ");

        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        stringBuilder.append("]");

        return stringBuilder.toString();
    }


}
