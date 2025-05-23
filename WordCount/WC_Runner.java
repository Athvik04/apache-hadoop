import java.io.IOException;     
import org.apache.hadoop.fs.Path;     
import org.apache.hadoop.io.IntWritable;     
import org.apache.hadoop.io.Text;     
import org.apache.hadoop.mapred.JobClient;     
import org.apache.hadoop.mapred.JobConf;     
import org.apache.hadoop.mapred.FileInputFormat;     
import org.apache.hadoop.mapred.FileOutputFormat;     
import org.apache.hadoop.mapred.TextInputFormat;     
import org.apache.hadoop.mapred.TextOutputFormat;    

public class WC_Runner {     
  public static void main(String[] args) throws IOException 
  {     
    JobConf conf = new JobConf(WC_Runner.class);     
    conf.setJobName("WordCount");     
    conf.setMapperClass(WC_Mapper.class);     
    conf.setCombinerClass(WC_Reducer.class);     
    conf.setReducerClass(WC_Reducer.class);          
    conf.setOutputKeyClass(Text.class);     
    conf.setOutputValueClass(IntWritable.class);             
    conf.setInputFormat(TextInputFormat.class);    //  
    conf.setOutputFormat(TextOutputFormat.class);  //        
    FileInputFormat.setInputPaths(conf,new Path("input.txt"));     
    FileOutputFormat.setOutputPath(conf,new Path("output"));      
    JobClient.runJob(conf);     
  }    
}    
