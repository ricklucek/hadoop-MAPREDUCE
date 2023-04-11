package TDE1.AveragePriceCommoditiesExportBrazil;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityYearFlowWritable implements WritableComparable<CommodityYearFlowWritable> {

    // Atributos privados
    private String commodity;

    private String year;

    private String flow;


    public CommodityYearFlowWritable() {
    }

    public CommodityYearFlowWritable(String commodity, String year, String flow) {
        this.commodity = commodity;
        this.year = year;
        this.flow = flow;
    }

    // GETTERS e SETTERS dos atributos
    public String getCommodity() {
        return commodity;
    }

    public String getYear() {
        return year;
    }

    public String getFlow() {
        return flow;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }


    public void setYear(String year) {
        this.year = year;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public int compareTo(CommodityYearFlowWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        year = dataInput.readUTF();
        flow = dataInput.readUTF();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommodityYearFlowWritable)) return false;
        CommodityYearFlowWritable that = (CommodityYearFlowWritable) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(year, that.year) && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, year, flow);
    }

    @Override
    public String toString() {
        return "CommodityYearFlowWritable{" +
                "commodity='" + commodity + '\'' +
                ", year='" + year + '\'' +
                ", flow='" + flow + '\'' +
                '}';
    }
}
