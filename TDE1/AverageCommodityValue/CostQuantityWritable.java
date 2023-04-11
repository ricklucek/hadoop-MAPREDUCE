package TDE1.AverageCommodityValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class CostQuantityWritable implements WritableComparable<CostQuantityWritable>{

    // Atributos privados

    Double cost;
    int qtd;

    // Constructors
    public CostQuantityWritable() {
    }

    public CostQuantityWritable(Double cost, int qtd) {
        this.cost = cost;
        this.qtd = qtd;
    }

    // GETTERS e SETTERS dos atributos

    public Double getCost() {
        return cost;
    }

    public void setCost(Double cost) {
        this.cost = cost;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(CostQuantityWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(cost);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cost = dataInput.readDouble();
        qtd = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CostQuantityWritable)) return false;
        CostQuantityWritable that = (CostQuantityWritable) o;
        return Objects.equals(cost, that.cost) && Objects.equals(qtd, that.qtd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cost, qtd);
    }
}
