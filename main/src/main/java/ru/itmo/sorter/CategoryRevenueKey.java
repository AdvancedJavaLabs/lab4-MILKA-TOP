package ru.itmo.sorter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryRevenueKey implements WritableComparable<CategoryRevenueKey> {
    private Text category;
    private DoubleWritable revenue;

    public CategoryRevenueKey() {
        category = new Text();
        revenue = new DoubleWritable();
    }

    public CategoryRevenueKey(String category, double revenue) {
        this.category = new Text(category);
        this.revenue = new DoubleWritable(revenue);
    }

    public String getCategory() {
        return category.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        category.write(dataOutput);
        revenue.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        category.readFields(dataInput);
        revenue.readFields(dataInput);
    }

    @Override
    public int compareTo(CategoryRevenueKey key) {
        int result = Double.compare(key.revenue.get(), this.revenue.get());
        if (result == 0) {
            return this.category.compareTo(key.category);
        }
        return result;
    }

    @Override
    public String toString() {
        return category + "\t" + revenue;
    }
}