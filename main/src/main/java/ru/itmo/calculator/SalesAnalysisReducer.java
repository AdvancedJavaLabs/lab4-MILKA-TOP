package ru.itmo.calculator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.itmo.common.SalesLineWritable;

import java.io.IOException;

public class SalesAnalysisReducer extends Reducer<Text, SalesLineWritable, Text, SalesLineWritable> {
    @Override
    protected void reduce(Text key, Iterable<SalesLineWritable> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        long totalQuantity = 0;

        for (SalesLineWritable value : values) {
            long quantity = value.getQuantity().get();
            double revenue = value.getPrice().get() * quantity;

            totalRevenue += revenue;
            totalQuantity += quantity;
        }

        context.write(key, new SalesLineWritable(totalRevenue, totalQuantity));
    }
}
