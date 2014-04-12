package cc.nlplab;

import lombok.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
@NoArgsConstructor
public class TermDfTermInfoArray implements Writable {
    @NonNull @Getter @Setter private String term;
    @NonNull @Getter @Setter private int df;
    @NonNull @Getter @Setter private TermInfoArray termInfos;

    @Override
    public void readFields(DataInput in) throws IOException {
        if (termInfos == null)
            termInfos = new TermInfoArray();
        Text termText = new Text();
        termText.readFields(in);
        this.term = termText.toString();
        this.df = in.readInt();
        termInfos.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(term).write(out);
        out.writeInt(df);
        termInfos.write(out);
    }

    @Override
    public String toString() {
        return "<tm=" + this.term + ", df=" + this.df + ", tmInfs=" + termInfos + ">";
    }



}


