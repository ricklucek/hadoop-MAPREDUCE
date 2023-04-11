package TDE1.FlowAndYear;

import advanced.customwritable.FireAvgTempWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowAndYearWritable
        implements WritableComparable<FlowAndYearWritable>{

    // Atributos privados
    private String flowType;
    private String year;

    public FlowAndYearWritable() {
    }

    public FlowAndYearWritable(String flowType, String year) {
        this.flowType = flowType;
        this.year = year;
    }


    // GET dos atributos
    public String getFlowType() {
        return flowType;
    }

    public String getYear() {
        return year;
    }


    public void set(String flowType, String year) {
        this.flowType = flowType;
        this.year = year;
    }

    @Override
    public int compareTo(FlowAndYearWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flowType = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(flowType);
        out.writeUTF(year);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlowAndYearWritable)) return false;
        FlowAndYearWritable that = (FlowAndYearWritable) o;
        return Objects.equals(flowType, that.flowType) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowType, year);
    }

    @Override
    public String toString() {
        return "FlowAndYearWritable{" +
                "flowType='" + flowType + '\'' +
                ", year='" + year + '\'' +
                '}';
    }
}
