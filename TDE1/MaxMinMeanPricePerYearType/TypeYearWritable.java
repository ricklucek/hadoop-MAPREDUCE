package TDE1.MaxMinMeanPricePerYearType;

import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class TypeYearWritable implements WritableComparable<TypeYearWritable> {


    private String commodity;
    private String year;


    public TypeYearWritable() {
    }


    public TypeYearWritable(String commodity, String year) {
        this.commodity = commodity;
        this.year = year;
    }


    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }


    @Override
    public int compareTo(TypeYearWritable o) {
        // Manter essa implementação independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeYearWritable)) return false;
        TypeYearWritable that = (TypeYearWritable) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commodity);
        out.writeUTF(year);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "TypeYearWritable{" +
                "commodity='" + commodity + '\'' +
                ", year='" + year + '\'' +
                '}';
    }
}


