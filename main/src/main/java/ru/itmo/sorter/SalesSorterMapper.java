package ru.itmo.sorter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.itmo.common.SalesLineWritable;

import java.io.IOException;

public class SalesSorterMapper extends Mapper<Object, Text, CategoryRevenueKey, SalesLineWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        System.out.println(value);
        if (fields.length == 3) {
            String categoryName = fields[0];
            double price = Double.parseDouble(fields[1]);
            long quantity = Long.parseLong(fields[2]);

            System.out.println(categoryName);
            context.write(new CategoryRevenueKey(categoryName, price), new SalesLineWritable(price, quantity));
        }
    }
}
