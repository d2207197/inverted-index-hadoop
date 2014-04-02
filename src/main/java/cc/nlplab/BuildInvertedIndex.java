package cc.nlplab;
 
import java.io.IOException;
import java.util.*;

import cc.nlplab.TextPair;
import cc.nlplab.KeyCount;
import cc.nlplab.KeyCountArrayWritable;
 
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


// import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
// import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;


public class BuildInvertedIndex  extends Configured implements Tool{
    private final Logger log = Logger.getLogger(getClass());

 
    public static class CountTfMap extends Mapper<LongWritable, Text, TextPair, KeyCount> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	Text fileName = new Text();
	protected void setup(Context context) throws IOException, InterruptedException
	    {
		fileName = new Text(((FileSplit) context.getInputSplit()).getPath().getName().toString());
 
	    }
 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    // StringTokenizer tokenizer = new StringTokenizer(line);

	    String[] terms = line.trim().split("[^a-zA-Z]");

	    int index = 0;
	    for (String term: terms){
		index++;
		Text termText = new Text(term.toLowerCase());
		context.write(new TextPair(termText, fileName), new KeyCount (fileName, one));
	    }
	}
    }


    // public class CountTfPartition extends Partitioner<TextPair, IntWritable>{
    // 	@Override
    // 	public int getPartition(TextPair termFile, IntWritable value, int numPartitions) {
    // 	    return  (termFile.getFirst ().hashCode () &Interger.MAX_VALUE) % numPartitions ;
    // 	}
    // }

    
    public static class CountTfCombine extends Reducer<TextPair, KeyCount, TextPair, KeyCount> {

	public void reduce (TextPair termFile, Iterable<KeyCount> fileTfs, Context context)
	    throws IOException, InterruptedException {
	    int sum = 0;
	    for (KeyCount fileTf: fileTfs) {
		sum += fileTf.getCount ().get ();
	    }

	    context.write(termFile, new KeyCount ( termFile.getSecond (), new IntWritable (sum) ));
	}
    }

    public static class GroupComparator extends WritableComparator{
	protected GroupComparator(){
	    super(TextPair.class, true);
	}
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
	    return ((TextPair)w1).getFirst ().compareTo (((TextPair)w2).getFirst ());
	}
    }

    public static class CountTfReduce extends Reducer<TextPair, KeyCount, KeyCount, KeyCountArrayWritable> {
	public void reduce (TextPair termFile, Iterable<KeyCount> fileTfs, Context context)
	    throws IOException, InterruptedException {

	    ArrayList<KeyCount> fileTfs_list = new ArrayList<KeyCount>();

	    for (KeyCount fileTf: fileTfs) {
		fileTfs_list.add ((KeyCount) WritableUtils.clone (fileTf, context.getConfiguration ()));
	    }
	    IntWritable df = new IntWritable(fileTfs_list.size());
	    KeyCount [] fileTfs_array = fileTfs_list.toArray(new KeyCount [fileTfs_list.size ()]);

	    context.write(new KeyCount(termFile.getFirst (), df), new KeyCountArrayWritable(fileTfs_array));
	}
    }

    public static class MergeTfMap extends Mapper<TextPair, IntWritable, Text, KeyCount> {
	public void map(TextPair termFile, IntWritable count, Context context) throws IOException, InterruptedException {
	    context.write(termFile.getFirst(), new KeyCount(termFile.getSecond(), count));
	}
    }
 
    public static class MergeTfReduce extends Reducer<Text, KeyCount, KeyCount, KeyCountArrayWritable> {
	public void reduce(Text term, Iterable<KeyCount> fileTfs, Context context)
	    throws IOException, InterruptedException {
	}
    }


    // public static class Reduce extends Reducer<EasyPair, IntWritable, EasyPair, IntWritable> {
 
    // 	public void reduce(EasyPair key, Iterable<IntWritable> values, Context context)
    // 	    throws IOException, InterruptedException {
    // 	    int sum = 0;
    // 	    for (IntWritable val : values) {
    // 		sum += val.get();
    // 	    }
    // 	    context.write(key, new IntWritable(sum));
    // 	}
    // }




    private Job countTfJob(Path inputPath, Path outputPath) throws IOException {

	Job job = new Job(getConf(), "step 1: count tf");
	job.setJarByClass(getClass());

	// input
	TextInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(TextInputFormat.class);

	// mapper
	job.setMapperClass(CountTfMap.class);
        job.setMapOutputKeyClass(TextPair.class);
	job.setMapOutputValueClass(KeyCount.class);

	// grouping
	job.setGroupingComparatorClass (GroupComparator.class);

	// combiner
	job.setCombinerClass(CountTfCombine.class);

	// reducer
	job.setReducerClass(CountTfReduce.class);
        job.setOutputKeyClass(KeyCount.class);
	job.setOutputValueClass(KeyCountArrayWritable.class);
 
	// job.setNumReduceTasks(8);
	// output
	// SequenceFileOutputFormat.setOutputPath(job, outputPath);
	// job.setOutputFormatClass(SequenceFileOutputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	TextOutputFormat.setOutputPath(job, outputPath);

	return job;
    }

    private Job mergeTfJob(Path inputPath, Path outputPath) throws IOException {

	Job job = new Job(getConf(), "step 2: merge tf");

	job.setJarByClass(BuildInvertedIndex.class);

	// input
	SequenceFileInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(SequenceFileInputFormat.class);

	// map
	job.setMapperClass(MergeTfMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(KeyCount.class);

	// reducer
	job.setReducerClass(MergeTfReduce.class);
        job.setOutputKeyClass(KeyCount.class);
	job.setOutputValueClass(KeyCountArrayWritable.class);
 
	// job.setNumReduceTasks(0);
	// output
	// job.setOutputFormatClass(SequenceFileOutputFormat.class);
	// SequenceFileOutputFormat.setOutputPath(job, outputPath);
	job.setOutputFormatClass(TextOutputFormat.class);
	TextOutputFormat.setOutputPath(job, outputPath);

	return job;
    }
    
    @Override
    public int run(String[] args) throws Exception {
	// Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(getConf());
	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);

	Path parentPath = outputPath.getParent();
	// Path tmp1Dir = new Path(parentDir, "tmp");
	Path intermediateTempPath = new Path(parentPath, getClass().getSimpleName() + "-tmp");
	
	fs.delete(intermediateTempPath);
	fs.delete(outputPath);

	try {

	    JobControl jobControler = new JobControl("build inverted index");

	    ControlledJob step1 = new ControlledJob(countTfJob(inputPath, outputPath), null);
	    // ControlledJob step2 = new ControlledJob(mergeTfJob(intermediateTempPath, outputPath), Arrays.asList(step1));
	    
	    jobControler.addJob(step1);
	    // jobControler.addJob(step2);
	    
	    Thread workflowThread = new Thread(jobControler, "Workflow-Thread");
	    workflowThread.setDaemon(true);
	    workflowThread.start();
	    
	    // jobControler.run();
	
	    while (!jobControler.allFinished()){
		Thread.sleep(500);
	    }
	    if ( jobControler.getFailedJobList().size() > 0 ){
		log.error(jobControler.getFailedJobList().size() + " jobs failed!");
		for ( ControlledJob job : jobControler.getFailedJobList()){
		    log.error(job.getJobName() + " failed");
		}
	    } else {
		log.info("Success!! Workflow completed [" + jobControler.getSuccessfulJobList().size() + "] jobs");
	    }

	} finally {
	    // fs.delete(intermediateTempPath);
	    System.out.println("Yeah");
	}
	return 0;

    } 
    public static void main(String[] args) throws Exception {

	int exitCode = ToolRunner.run(new BuildInvertedIndex(), args);
	System.exit(exitCode);



	// Set the outputs for the Map
        // job.setMapOutputValueClass(IntWritable.class);

        // Set the outputs for the Job
 
	// job1.waitForCompletion(true);
    }
 
}
