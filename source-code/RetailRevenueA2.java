import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RetailRevenueA2 {

    public static class RevenueMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text productCategory = new Text();
        private DoubleWritable priceValue = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header
            if (line.startsWith("transaction_id")) {
                return;
            }

            String[] fields = line.split(",");

            try {
                String category = fields[2].trim();
                double price = Double.parseDouble(fields[3].trim());

                productCategory.set(category);
                priceValue.set(price);

                context.write(productCategory, priceValue);

            } catch (Exception e) {
                // Skip bad rows
            }
        }
    }

    public static class RevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Retail Revenue Assignment 2");

        job.setJarByClass(RetailRevenueA2.class);
        job.setMapperClass(RevenueMapper.class);
        job.setReducerClass(RevenueReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}