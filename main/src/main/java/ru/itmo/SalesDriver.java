package ru.itmo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.itmo.calculator.SalesAnalysisMapper;
import ru.itmo.calculator.SalesAnalysisReducer;
import ru.itmo.common.SalesLineWritable;
import ru.itmo.sorter.CategoryRevenueKey;
import ru.itmo.sorter.SalesSorterMapper;
import ru.itmo.sorter.SalesSorterReducer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

// zip -d out/artifacts/main_main_jar/main.main.jar 'META-INF/.SF' 'META-INF/.RSA' 'META-INF/*SF'
// hadoop jar out/artifacts/main_main_jar/main.main.jar ru.itmo.SalesDriver ./input ./output
public class SalesDriver {
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length > 4 || args.length < 2) {
            System.err.println("Usage: SalesDriver <input path> <output path> <num reducers> <split maxsize>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        if (args.length >= 3) {
            conf.setInt("mapreduce.job.reduces", Integer.parseInt(args[2]));
        }
        if (args.length == 4) {
            conf.setLong("mapreduce.input.fileinputformat.split.maxsize", Long.parseLong(args[3]));
        }

        FileSystem fs = FileSystem.get(conf);

        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        Path tempPath = new Path(args[1] + "--tmp");
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }


        runSalesAnalysis(args, conf, tempPath);

        var jobSorter = runSalesSorter(conf, tempPath, outputPath);
        if (!jobSorter.waitForCompletion(true)) {
            System.exit(1);
        }

        Long endTime = System.currentTimeMillis() - start;

        var file = new File(outputPath.toString(), "execution_time.log");
        try (FileWriter writer = new FileWriter(file, false)) {
            writer.write(endTime.toString());
            System.out.println("Execution time logged successfully to " + file);
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    private static void runSalesAnalysis(String[] args, Configuration conf, Path tempPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job jobAnalysis = Job.getInstance(conf, "Sales Analysis");

        jobAnalysis.setJarByClass(SalesDriver.class);
        jobAnalysis.setMapperClass(SalesAnalysisMapper.class);
        jobAnalysis.setReducerClass(SalesAnalysisReducer.class);

        jobAnalysis.setOutputKeyClass(Text.class);
        jobAnalysis.setOutputValueClass(SalesLineWritable.class);

        FileInputFormat.addInputPath(jobAnalysis, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobAnalysis, tempPath);

        if (!jobAnalysis.waitForCompletion(true)) System.exit(1);
    }

    private static Job runSalesSorter(Configuration conf, Path tempPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job jobSorter = Job.getInstance(conf, "Sales Sorter");

        jobSorter.setJarByClass(SalesDriver.class);
        jobSorter.setMapperClass(SalesSorterMapper.class);
        jobSorter.setReducerClass(SalesSorterReducer.class);

        jobSorter.setMapOutputKeyClass(CategoryRevenueKey.class);
        jobSorter.setMapOutputValueClass(SalesLineWritable.class);

        jobSorter.setOutputKeyClass(Text.class);
        jobSorter.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(jobSorter, tempPath);
        FileOutputFormat.setOutputPath(jobSorter, outputPath);

        return jobSorter;
    }
}
