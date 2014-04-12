package cc.nlplab;

import java.lang.reflect.*;

import lombok.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


// @NoArgsConstructor
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
public class PairWC<F extends WritableComparable, S extends WritableComparable>  implements WritableComparable {
    @NonNull @Getter @Setter private Class<? extends WritableComparable> firstClass;
    @NonNull @Getter @Setter private Class<? extends WritableComparable> secondClass;
    @NonNull @Getter @Setter private F first;
    @NonNull @Getter @Setter private S second;

    public PairWC(@NonNull final Class<? extends WritableComparable> firstClass, @NonNull final Class<? extends WritableComparable> secondClass) {
        this.firstClass = firstClass;
        this.secondClass = secondClass;
    }

    public int compareTo(Object object) {
        PairWC ip2 = (PairWC) object;
        int cmp = getFirst().compareTo(ip2.getFirst());
        if (cmp != 0)
            return cmp;
        return getSecond().compareTo(ip2.getSecond()); // reverse
    }
    public void readFields(DataInput in) throws IOException {

        try {
            if (first == null)
                first = (F)firstClass.newInstance();
            if (second == null)
                second = (S)secondClass.newInstance();
        } catch (java.lang.InstantiationException e) {
            System.out.println("nothing");
        } catch (java.lang.IllegalAccessException e) {
            System.out.println("nothing");
        }
        first.readFields(in);
        second.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    public java.lang.String toString() {
        return this.getClass().getSimpleName() + "(" + this.getFirst() + ", " + this.getSecond() + ")";
    }
}


