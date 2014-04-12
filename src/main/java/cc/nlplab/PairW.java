package cc.nlplab;

import java.lang.reflect.*;

import lombok.*;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


// @NoArgsConstructor
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
public class PairW<F extends Writable, S extends Writable> implements Writable {
    @NonNull @Getter @Setter private Class<? extends Writable> firstClass;
    @NonNull @Getter @Setter private Class<? extends Writable> secondClass;
    @NonNull @Getter @Setter private F first;
    @NonNull @Getter @Setter private S second;

    public PairW(@NonNull final Class<? extends Writable> firstClass, @NonNull final Class<? extends Writable> secondClass) {
        this.firstClass = firstClass;
        this.secondClass = secondClass;
    }

    // public int compareTo(Object object) {
    //   PairW ip2 = (PairW) object;
    //   int cmp = getFirst().compareTo(ip2.getFirst());
    //   if (cmp != 0)
    //     return cmp;
    //   return getSecond().compareTo(ip2.getSecond()); // reverse
    // }
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


