import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Question2 {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {
    private final static IntWritable wr = new IntWritable(1);
    private Text area = new Text();

    private String val = null;
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] myarray = line.split("::");
      
	
  	if(Integer.parseInt(myarray[2]) <= 18)
	{
     val = "7 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
	 }
	 else if(Integer.parseInt(myarray[2]) > 18 && Integer.parseInt(myarray[2]) <= 24)
	 {
	 val = "24 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 25 && Integer.parseInt(myarray[2]) <= 34){
	  val = "31 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 35 && Integer.parseInt(myarray[2]) <= 44){
	  val = "41 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 45 && Integer.parseInt(myarray[2]) <= 55){
	  val = "51 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 56 && Integer.parseInt(myarray[2]) <= 61){
	  val = "56 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
	 }
	 else
	 {
	  val = "62 "+myarray[1];
     area.set(val);
     output.collect(area, wr);
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
    JobConf conf = new JobConf(Question2.class);
    
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

