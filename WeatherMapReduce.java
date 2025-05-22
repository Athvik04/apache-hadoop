//4
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherMapReduce {
    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
        private final static DoubleWritable temperature = new DoubleWritable();
        private Text date = new Text();
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            if (line.length == 3) {
                date.set(line[0]);
                temperature.set(Double.parseDouble(line[2]));
                context.write(date, temperature);
            }
        }
    }
     
    public static class WeatherReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) 
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = sum / count;
            context.write(key, new DoubleWritable(avg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather analysis");
        job.setJarByClass(WeatherMapReduce.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);  //no combiner class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path("input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true)?0 : 1);
    }
}

2024-01-01, karnataka, 25.0
2024-01-02, karnataka, 26.0
2024-01-03, karnataka, 27.0
