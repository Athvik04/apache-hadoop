// 1 

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1); //setting initial value to 1
    private Text word = new Text(); //creating a new Text object to store the word
    //initially all words are mapped to 1 [key, value] = [word, 1]

    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, IntWritable> output, 
                    Reporter reporter)
            throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) { //if more tokens are present
            word.set(tokenizer.nextToken());
            output.collect(word, one); // creating initial key-value pair for all words
        }
    }
}