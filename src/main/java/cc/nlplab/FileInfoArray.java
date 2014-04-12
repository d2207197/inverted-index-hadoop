package cc.nlplab;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;


import org.apache.hadoop.io.ArrayWritable;
// import org.apache.hadoop.io.IntWritable;
import cc.nlplab.TermInfo;

public class FileInfoArray extends ArrayWritable {
    private HashSet<String> notFileNameSet = new HashSet<String>();
    private boolean notFlag= false;

    public FileInfoArray() {
        super(FileInfo.class);
    }
    public FileInfoArray(FileInfo[] values) {
        super(FileInfo.class, values);
    }

    public FileInfoArray (ArrayList<FileInfo> values) {
        super(FileInfo.class);
        FileInfo [] values_A = values.toArray(new FileInfo[values.size()]);
        this.set(values_A);
    }
    private FileInfoArray filterBySet(FileInfoArray data, FileInfoArray filter) {
        FileInfo [] fileInfos = (FileInfo [])data.get();
        ArrayList<FileInfo> result = new ArrayList<FileInfo>();
        for (FileInfo fileInfo: fileInfos)
            if( ! filter.notFileNameSet.contains(fileInfo.getFileName()))
                result.add(fileInfo);
        FileInfo [] result_A = result.toArray(new FileInfo[result.size()]);
        return new FileInfoArray(result_A);

    }
    public FileInfoArray and(FileInfoArray other) {
        if (this.notFlag && other.notFlag ) {
            this.notFileNameSet.addAll(other.notFileNameSet);
            return this;
        } else if (this.notFlag ) {
            return filterBySet(other, this);
        } else if(other.notFlag) {
            return filterBySet(this, other);
        }

        Writable [] ours = this.get();
        Writable [] theirs = other.get();
        ArrayList<FileInfo> result = new ArrayList<FileInfo>();

        int oi = 0;
        int ti = 0;
        while(oi < ours.length && ti < theirs.length) {
            FileInfo our = (FileInfo)ours[oi];
            FileInfo their = (FileInfo)theirs[ti];
            int res = our.getFileName().compareTo(their.getFileName());
            if ( res == 0) {
                double score = our.getScore() + their.getScore();
                our.getTermOffsets().addAll(their.getTermOffsets());
                result.add(new FileInfo(their.getFileName(), score, our.getTermOffsets()));

                ti++;
                oi++;
            } else if ( res > 0 ) ti++;
            else oi++;
        }

        FileInfo [] result_A = result.toArray(new FileInfo[result.size()]);
        return new FileInfoArray(result_A);
    }

    public FileInfoArray or(FileInfoArray other) {
        FileInfo [] ours = (FileInfo [])this.get();
        FileInfo [] theirs = (FileInfo [])other.get();
        ArrayList<FileInfo> result = new ArrayList<FileInfo>();

        int oi = 0;
        int ti = 0;
        while(oi < ours.length && ti < theirs.length) {
            FileInfo our = ours[oi];
            FileInfo their = theirs[ti];
            int res = our.getFileName().compareTo(their.getFileName());

            if (res == 0) {
                double score = (our.getScore() > their.getScore())? our.getScore(): their.getScore();
                our.getTermOffsets().addAll(their.getTermOffsets());
                result.add(new FileInfo(their.getFileName(), score, our.getTermOffsets()));
                ti++;
                oi++;
            } else {
                result.add(our);
                result.add(their);
                ti++;
                oi++;
            }

        }

        if (oi < ours.length)
            result.addAll(Arrays.asList(Arrays.copyOfRange(ours, oi ,ours.length)));
        if (ti < theirs.length)
            result.addAll(Arrays.asList(Arrays.copyOfRange(theirs, ti ,theirs.length)));

        // System.out.println("length of OR result: " + result.size());
        FileInfo [] result_A = result.toArray(new FileInfo[result.size()]);
        return new FileInfoArray(result_A);
    }

    public FileInfoArray not() {

        // System.out.println("#1");
        FileInfo [] fileInfos = (FileInfo []) this.get();
        // System.out.println("#2");
        if(notFileNameSet == null) {
            // System.out.println("#3");
            notFileNameSet = new HashSet<String>();
        }
        // System.out.println("#4");


        for (FileInfo fileInfo: fileInfos) {
            // System.out.println("#5");
            notFileNameSet.add(fileInfo.getFileName());
        }

        // System.out.println("#6");
        notFlag = true;
        // this.set(new FileInfo [0]);
        return this;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (notFlag) {
            stringBuilder.append("-");
            stringBuilder.append(getClass().getSimpleName() + "[");
            for(String fn: notFileNameSet){
                stringBuilder.append(fn + ", ");
            }

        }
        else {

            stringBuilder.append(getClass().getSimpleName() + "[");

            if (this.get().length == 0) {
                stringBuilder.append("]");
                return stringBuilder.toString();
            }
            

            for (Writable entry : this.get()) {
                FileInfo fileInfo = (FileInfo) entry;
                // System.out.println("entry: " + fileInfo);
                stringBuilder.append(fileInfo.getFileName() + ", ");
            }
            // stringBuilder.append(entry.getKey().toString() + "=" + entry.getValue().toString() + ", ");
            
        }

        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        stringBuilder.append("]");

        return stringBuilder.toString();
    }


}
