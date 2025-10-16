import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class solution3 {

    // mapper: turns each line into a key-value pair
    public static class TokenizerMapper
         extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable number = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return; // skip empty lines

            StringTokenizer itr = new StringTokenizer(line);
            if (itr.countTokens() < 2) return; // skip lines without enough data

            String keyStr = itr.nextToken();
            String valStr = itr.nextToken();

            try {
                int val = Integer.parseInt(valStr); // convert string to int
                word.set(keyStr);
                number.set(val);
                context.write(word, number); // send to reducer
            } catch (NumberFormatException e) {
                // skip if value is not a number
            }
        }
    }

    // reducer: calculates max, min, sum, avg for each key
    public static class StatsReducer
         extends Reducer<Text,IntWritable,NullWritable,Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            int count = 0;

            for (IntWritable val : values) {
                int v = val.get();
                if (v > max) max = v; // update max
                if (v < min) min = v; // update min
                sum += v; // add to sum
                count++;  // count number of values
            }

            if (count == 0) return; // skip keys with no valid numbers

            double avg = (double) sum / count; // calculate average

            // align output: item name max 20 chars, numbers 10 chars each
            String stats = String.format("%-20s%-10s%-10s%-10s%-10s",
                    key.toString(),
                    "Max:" + max,
                    "Min:" + min,
                    "Avg:" + Math.round(avg),
                    "Total:" + sum
            );

            result.set(stats);
            context.write(NullWritable.get(), result);
        }
    }

    // main: configure job and run
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: solution3 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MinMax Extended");
        job.setJarByClass(solution3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(StatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
