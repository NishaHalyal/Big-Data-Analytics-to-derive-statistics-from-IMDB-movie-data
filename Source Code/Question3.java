import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;

public class Question3 {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {
    private final static IntWritable wr = new IntWritable(1);
    private Text area = new Text();

    private static String gen; 
    private String val = null;
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        gen= job.get("third_args");
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] myarray = line.split("::");
      String[]  genre=myarray[2].split("\\|");
    	for(int i=0;i<=genre.length-1;i++)
    	{
    		if(genre[i].equalsIgnoreCase(gen))
    		{
    			val=myarray[1];
    			area.set(val);
    			output.collect(area, wr);
    		}
    	}
    }
  
 }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Question3.class);
    conf.set("third_args", args[2]);
    conf.setJobName("regionCount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}

