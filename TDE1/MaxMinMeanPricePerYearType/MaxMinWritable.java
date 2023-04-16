package TDE1.MaxMinMeanPricePerYearType;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class MaxMinWritable implements WritableComparable<MaxMinWritable>{

    private double Max;

    private double Min;

    private float Average;

    public MaxMinWritable() {
    }

    public MaxMinWritable(double max, double min, float average) {
        this.Max = max;
        this.Min = min;
        this.Average = average;

    }

    public double getMax() {
        return Max;
    }

    public void setMax(double max) {
        Max = max;
    }

    public double getMin() {
        return Min;
    }

    public void setMin(double min) {
        Min = min;
    }

    public float getAverage() {
        return Average;
    }

    public void setAverage(float average) {
        Average = average;
    }

    @Override
    public int compareTo(MaxMinWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MaxMinWritable)) return false;
        MaxMinWritable that = (MaxMinWritable) o;
        return Double.compare(that.Max, Max) == 0 && Double.compare(that.Min, Min) == 0 && Float.compare(that.Average, Average) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Max, Min, Average);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(Max);
        dataOutput.writeDouble(Min);
        dataOutput.writeFloat(Average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Max = dataInput.readDouble();
        Min = dataInput.readDouble();
        Average = dataInput.readFloat();
    }
}
