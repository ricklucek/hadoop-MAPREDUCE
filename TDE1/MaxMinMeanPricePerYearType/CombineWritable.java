package TDE1.MaxMinMeanPricePerYearType;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CombineWritable implements WritableComparable<CombineWritable>{

    private double Max;

    private double Min;

    private double somaCusto;

    private int somaQtd;

    public CombineWritable() {
    }

    public CombineWritable(double max, double min, double somaCusto, int somaQtd) {
        this.Max = max;
        this.Min = min;
        this.somaCusto = somaCusto;
        this.somaQtd = somaQtd;
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

    public double getSomaCusto() {
        return somaCusto;
    }

    public void setSomaCusto(double somaCusto) {
        this.somaCusto = somaCusto;
    }

    public int getSomaQtd() {
        return somaQtd;
    }

    public void setSomaQtd(int somaQtd) {
        this.somaQtd = somaQtd;
    }

    @Override
    public int compareTo(CombineWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CombineWritable)) return false;
        CombineWritable that = (CombineWritable) o;
        return Double.compare(that.Max, Max) == 0 && Double.compare(that.Min, Min) == 0 && Double.compare(that.somaCusto, somaCusto) == 0 && somaQtd == that.somaQtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Max, Min, somaCusto, somaQtd);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(Max);
        dataOutput.writeDouble(Min);
        dataOutput.writeDouble(somaCusto);
        dataOutput.writeInt(somaQtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Max = dataInput.readDouble();
        Min = dataInput.readDouble();
        somaCusto = dataInput.readDouble();
        somaQtd = dataInput.readInt();
    }
}

