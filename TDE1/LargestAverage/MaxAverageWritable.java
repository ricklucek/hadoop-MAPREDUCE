package TDE1.LargestAverage;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxAverageWritable implements WritableComparable<MaxAverageWritable>{

    private float Average;

    public MaxAverageWritable() {
    }

    public MaxAverageWritable(float average) {
        this.Average = average;

    }

    public float getAverage() {
        return Average;
    }

    public void setAverage(float average) {
        Average = average;
    }

    @Override
    public int compareTo(MaxAverageWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MaxAverageWritable)) return false;
        MaxAverageWritable that = (MaxAverageWritable) o;
        return Float.compare(that.Average, Average) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Average);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(Average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Average = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "AverageWritable{" +
                ", Average=" + Average +
                '}';
    }
}
