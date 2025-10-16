// solution2.java
// MapReduce program to calculate the average speed of cars exceeding 90 km/h
// at each camera location.

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class solution2 {

    // ---------- Mapper Class ----------
    public static class SpeedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static int SPEED_LIMIT = 90;
        private Text carLocation = new Text();
        private IntWritable speed = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 4) {
                String carReg = fields[0];
                String location = fields[1];
                int carSpeed = Integer.parseInt(fields[3]);

                if (carSpeed > SPEED_LIMIT) {
                    carLocation.set(carReg + " - " + location);
                    speed.set(carSpeed);
                    context.write(carLocation, speed);
                }
            }
        }
    }

    // ---------- Reducer Class ----------
    public static class SpeedReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            int average = sum / count;
            context.write(key, new Text("Speed limit exceeded!! Average Speed : " + average));
        }
    }

    // ---------- Main Driver ----------
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
