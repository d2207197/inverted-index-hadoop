package cc.nlplab;

import java.io.IOException;


import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// import cc.nlplab.TextPair;
import cc.nlplab.KeyCount;
import cc.nlplab.KeyCountArrayWritable;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Retrieval  extends Configured implements Tool{


  

  public static class RetrievalReducer extends Reducer<NullWritable, KeyCountArrayWritable, NullWritable, ArrayWritable> {

    // public void reduce(KeyCount, Iterable<KeyCountArrayWritable)
  }

  public static class filterQueryMapper extends Mapper<KeyCount, KeyCountArrayWritable, NullWritable, KeyCountArrayWritable> 
  {
    private Text query = new Text();
    
    protected void setup(Mapper.Context context) throws IOException, InterruptedException 
      {
	Configuration conf = context.getConfiguration();

	System.out.println(conf.get("query"));
	query.set( conf.get("query"));
	
      }

    public void map(KeyCount key, KeyCountArrayWritable value, Context context) throws IOException, InterruptedException 
      {
	if (key.getKey().compareTo(query) == 0)
	  context.write(NullWritable.get(), value);
      }
  }


  private final Logger log = Logger.getLogger(getClass());
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    System.out.println(args[1]);
    conf.set("query", args[1]);

    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(args[0]);
    Path parentPath = inputPath.getParent();
    Path outputPath = new Path(parentPath, getClass().getSimpleName());
    fs.delete(outputPath);


    Job job = new Job(getConf(), "Retrieval");

    job.setJarByClass(getClass());

	
    // input
    SequenceFileInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(SequenceFileInputFormat.class);

    // map
    job.setMapperClass(filterQueryMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(KeyCountArrayWritable.class);

    // reducer
    // job.setReducerClass(RetrievalReducer.class);
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(KeyCountArrayWritable.class);
 
    job.setNumReduceTasks(0);
    // output
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputPath);


    job.waitForCompletion(true);

    return 0;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Retrieval(), args);
    System.exit(exitCode);
  }

}
