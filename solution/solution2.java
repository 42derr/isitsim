import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class solution2 {

    // Mapper: emit (car-location, speed) if speed > 90
    public static class SpeedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static int SPEED_LIMIT = 90;
        private Text carLocation = new Text();
        private IntWritable speed = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 4) {
                int carSpeed = Integer.parseInt(fields[3]);
                if (carSpeed > SPEED_LIMIT) {
                    carLocation.set(fields[0] + " - " + fields[1]);
                    speed.set(carSpeed);
                    context.write(carLocation, speed);
                }
            }
        }
    }

    // Reducer: calculate average speed for each car-location
    public static class SpeedReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            int average = sum / count;
            context.write(key, new Text("Speed limit exceeded!! Average Speed : " + average));
        }
    }

    // Main: configure and run the job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: solution2 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Speed of Cars Exceeding Limit");
        job.setJarByClass(solution2.class);
        job.setMapperClass(SpeedMapper.class);
        job.setReducerClass(SpeedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
