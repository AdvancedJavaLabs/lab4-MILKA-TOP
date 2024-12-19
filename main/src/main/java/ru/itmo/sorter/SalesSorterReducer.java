package ru.itmo.sorter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.itmo.common.SalesLineWritable;

import java.io.IOException;

public class SalesSorterReducer extends Reducer<CategoryRevenueKey, SalesLineWritable, Text, Text> {
    @Override
    protected void reduce(CategoryRevenueKey key, Iterable<SalesLineWritable> values, Context context) throws IOException, InterruptedException {
        for (SalesLineWritable value : values) {
            context.write(new Text(key.getCategory()), new Text(String.format("%.2f\t%d", value.getPrice().get(), value.getQuantity().get())));
        }
    }
}
