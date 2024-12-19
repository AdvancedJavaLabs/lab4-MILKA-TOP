package ru.itmo.common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesLineWritable implements Writable {

    private DoubleWritable price;
    private LongWritable quantity;

    public SalesLineWritable() {
        price = new DoubleWritable();
        quantity = new LongWritable();
    }

    public SalesLineWritable(double price, long quantity) {
        this.price = new DoubleWritable(price);
        this.quantity = new LongWritable(quantity);
    }

    public DoubleWritable getPrice() {
        return price;
    }

    public LongWritable getQuantity() {
        return quantity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        price.write(dataOutput);
        quantity.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        price.readFields(dataInput);
        quantity.readFields(dataInput);
    }

    @Override
    public String toString() {
        return price + "\t" + quantity;
    }
}
