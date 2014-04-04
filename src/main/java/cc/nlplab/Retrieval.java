package cc.nlplab;

// import java.io.IOException;
import java.io.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.lang.Math;
import java.util.Collections;


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
import cc.nlplab.Pair;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.commons.beanutils.BeanComparator;




public class Retrieval  extends Configured implements Tool{


    public static double tf_idf(double tf,double df){
	int N = 44;
	
	return tf * Math.log(N/df);
    }

    public static HashMap<String, Integer> parseQuery(String query)
    {

	HashMap<String, Integer> queryMap = new HashMap<String, Integer>();
		
	int index = 0;
	for (String term : query.split(" "))
	    queryMap.put(term, index++);

	return queryMap;

		
    }


    public static class RetrievalReducer extends Reducer<NullWritable, TermDfFileTfArray, NullWritable, ArrayWritable>
    {
	private static String query;
	private static HashMap<String, Integer> queryMap ;
	Logger log = Logger.getLogger(RetrievalReducer.class);

	protected void setup(Mapper.Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		query = conf.get("query");
		queryMap = parseQuery(query);
	}
	
	public void reduce (NullWritable none, Iterable<TermDfFileTfArray> termDfFileTfArrays, Context context) throws IOException, InterruptedException
	{
	    ArrayList<Pair<Double, String>> result = new ArrayList<Pair<Double, String>>();
	    for (TermDfFileTfArray termDfFileTfArray: termDfFileTfArrays )
		{
		    KeyCount termDf = termDfFileTfArray.getTermDf();
		    KeyCountArrayWritable fileTfs = termDfFileTfArray.getFileTfArray();
		    int df = termDf.getCount().get();
		    for (Writable fileTf: fileTfs.get())
			{
			    String fileName = ((KeyCount)fileTf).getKey().toString();
			    int tf = ((KeyCount)fileTf).getCount().get();
			    result.add(Pair.of( tf_idf(tf, df), fileName));
			}
		    // for termDfFileTfArrays.getFileTfArray().get();
		}
	    BeanComparator fieldComparator = new BeanComparator("first");
	    Collections.sort(result, fieldComparator);

	    Text [] result_= new Text[result.size()];
	    int idx = 0;
	    for (Pair<Double, String> fileTf: result)
		result_[idx++] = new Text(fileTf.getSecond());
	    context.write(NullWritable.get(), new ArrayWritable (Text.class, result_ ));
	}

    }

    public static class filterQueryMapper extends Mapper<KeyCount, KeyCountArrayWritable, NullWritable, TermDfFileTfArray> 
    {
	private static String query;
	private static HashMap<String, Integer> queryMap ;
	Logger log = Logger.getLogger(filterQueryMapper.class);

	protected void setup(Mapper.Context context) throws IOException, InterruptedException
	    {
		Configuration conf = context.getConfiguration();
		query = conf.get("query");
		queryMap = parseQuery(query);
	    }

	public void map(KeyCount term, KeyCountArrayWritable value, Context context) throws IOException, InterruptedException 
	    {
		String termString = term.getKey().toString();
		if (queryMap.containsKey(termString))
		    context.write(NullWritable.get(), new TermDfFileTfArray( term, value));
	    }
    }


    private final Logger log = Logger.getLogger(getClass());
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	conf.set("query", args[1]);
	// System.out.println(args[1]);
	// ByteArrayOutputStream out = new ByteArrayOutputStream();
        // ObjectOutputStream objOut = new ObjectOutputStream(out);
        // objOut.writeObject(queryMap);
        // objOut.close();
	// conf.set("query", out.toString("UTF-8"));


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
	job.setMapOutputValueClass(TermDfFileTfArray.class);

	// reducer
	job.setReducerClass(RetrievalReducer.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(ArrayWritable.class);
 
	job.setNumReduceTasks(1);
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
