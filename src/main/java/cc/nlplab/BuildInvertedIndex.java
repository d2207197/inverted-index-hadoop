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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import org.apache.commons.cli.*;


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
 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    {
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


    private Job countTfJob(Path inputPath, Path outputPath, Boolean doTextOutput) throws IOException {

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
	if (doTextOutput == true){
	    TextOutputFormat.setOutputPath(job, outputPath);
	    job.setOutputFormatClass(TextOutputFormat.class);
	} else {
	    SequenceFileOutputFormat.setOutputPath(job, outputPath);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	}

	return job;
    }

        
    @Override
    public int run(String[] args) throws Exception {
	// Configuration conf = new Configuration();
	Options options = new Options();
	options.addOption("t", false, "output in text format");
	CommandLineParser parser = new GnuParser();
	CommandLine cmd = parser.parse( options, args);


	FileSystem fs = FileSystem.get(getConf());
	Path inputPath = new Path(cmd.getArgs()[0]);
	Path outputPath = new Path(cmd.getArgs()[1]);
	fs.delete(outputPath);

	Boolean doTextOutput = false;
	if(cmd.hasOption("t")) 
	    doTextOutput = true;
	    
	return (countTfJob(inputPath, outputPath, doTextOutput).waitForCompletion(true) ? 1: 0);
    } 

    public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new BuildInvertedIndex(), args);
	System.exit(exitCode);
    }
 
}
