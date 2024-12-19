package ru.itmo.calculator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.itmo.common.SalesLineWritable;

import java.io.IOException;

public class SalesAnalysisMapper extends Mapper<LongWritable, Text, Text, SalesLineWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("transaction_id")) return;

        String[] fields = line.split(",");

        if (fields.length == 5) {
            String category = fields[2];
            double price = Double.parseDouble(fields[3]);
            long quantity = Long.parseLong(fields[4]);

            context.write(new Text(category), new SalesLineWritable(price, quantity));
        }
    }
}
