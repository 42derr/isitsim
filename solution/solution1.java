import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class solution1 {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: solution1 <input1> <input2> <output>");
            System.exit(1);
        }

        String inputPath1 = args[0];
        String inputPath2 = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream out = fs.create(new Path(outputPath));

        // Merge the first input file
        FSDataInputStream in1 = fs.open(new Path(inputPath1));
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = in1.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }
        in1.close();

        // Merge the second input file
        FSDataInputStream in2 = fs.open(new Path(inputPath2));
        while ((bytesRead = in2.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }
        in2.close();

        out.close();
        fs.close();

        System.out.println("Files merged successfully into " + outputPath);
    }
}
