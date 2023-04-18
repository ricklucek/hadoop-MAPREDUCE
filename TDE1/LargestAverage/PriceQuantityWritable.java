package TDE1.LargestAverage;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class PriceQuantityWritable implements WritableComparable<PriceQuantityWritable> {


    private double commodityusd;
    private int qtd;


    public PriceQuantityWritable() {
    }


    public PriceQuantityWritable(double commodityusd, int qtd) {
        this.commodityusd = commodityusd;
        this.qtd = qtd;
    }

    public double getCommodityusd() {
        return commodityusd;
    }

    public void setCommodityusd(double commodityusd) {
        this.commodityusd = commodityusd;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(PriceQuantityWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PriceQuantityWritable)) return false;
        PriceQuantityWritable that = (PriceQuantityWritable) o;
        return Double.compare(that.commodityusd, commodityusd) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodityusd, qtd);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(commodityusd);
        out.writeInt(qtd);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodityusd = dataInput.readDouble();
        qtd = dataInput.readInt();
    }
}


