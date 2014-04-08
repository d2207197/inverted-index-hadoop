package cc.nlplab;

// import java.io.IOException;
import java.io.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.lang.Math;
import java.util.Collections;
import java.util.Map;


import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;



import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// import cc.nlplab.TextPair;
import cc.nlplab.TextIntWC;
import cc.nlplab.KeyCountArrayWritable;
import cc.nlplab.Pair;
import cc.nlplab.QueryHandler;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.commons.beanutils.BeanComparator;



import org.apache.hadoop.io.DoubleWritable;

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

    public static class filterQueryMapper extends Mapper<TextIntWC, SortedMapW, NullWritable, TextIntWithSortedMap> 
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

	public void map(TextIntWC termDf, SortedMapW fileTfMap, Context context) throws IOException, InterruptedException 
	    {
		String termString = termDf.getFirst().toString();
		if (queryMap.containsKey(termString))
		  context.write(NullWritable.get(), new TextIntWithSortedMap( termDf, fileTfMap));
	    }
    }


    public static class RetrievalReducer extends Reducer<NullWritable, TextIntWithSortedMap, NullWritable, FileTfIdfMap>
    {
	private static String query;
	private static HashMap<String, Integer> queryMap ;
	Logger log = Logger.getLogger(RetrievalReducer.class);

	protected void setup(Reducer.Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		query = conf.get("query");
		// queryMap = parseQuery(query);
	}
	
	public void reduce (NullWritable none, Iterable<TextIntWithSortedMap> termDf_FileTfMap_iter, Context context) throws IOException, InterruptedException
	{
	    // ArrayList<Pair<Double, String>> result = new ArrayList<Pair<Double, String>>();

	    // SortedMapW fileTfIdfMap = new SortedMapW();
	    QueryHandler queryhandler = new QueryHandler();

	    for (TextIntWithSortedMap termDf_FileTfMap: termDf_FileTfMap_iter )
		{
		    TextIntWC termDf = termDf_FileTfMap.getFirst();
		    SortedMapW fileTfMap = termDf_FileTfMap.getSecond();
		    int df = termDf.getSecond().get();

		    FileTfIdfMap fileTfIdfMap= new FileTfIdfMap();
		    for (Map.Entry<WritableComparable, Writable> fileTf : fileTfMap.entrySet())
		      {
			Text fileName = (Text)fileTf.getKey();
			int tf = ((IntWritable)fileTf.getValue()).get();
			// tfIdfMap.containsKey();
			
			fileTfIdfMap.put(fileName, new DoubleWritable(tf_idf(tf, df)));
		      }
		    queryhandler.putfileTfIdfMap(termDf.getFirst().toString(), fileTfIdfMap);

		    // for (Writable fileTf: fileTfMap.get())
		    // 	{
		    // 	    String fileName = ((TextIntWC)fileTf).getFirst().toString();
		    // 	    int tf = ((TextIntWC)fileTf).getSecond().get();
		    // 	    result.add(Pair.of( tf_idf(tf, df), fileName));
		    // 	}
		    // for termDfFileTfArrays.getFileTfArray().get();
		}

	    System.out.println("Start parsing: " + query );
	    FileTfIdfMap output = queryhandler.parser.parse(query);


	    

	    context.write(NullWritable.get(), output);
	    // BeanComparator fieldComparator = new BeanComparator("first");
	    // Collections.sort(result, fieldComparator);

	    // Text [] result_= new Text[result.size()];
	    // int idx = 0;
	    // for (Pair<Double, String> fileTf: result)
		// result_[idx++] = new Text(fileTf.getSecond());
	    // context.write(NullWritable.get(), new ArrayWritable (Text.class, result_ ));
	    
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
	job.setMapOutputValueClass(TextIntWithSortedMap.class);

	// reducer
	job.setReducerClass(RetrievalReducer.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(FileTfIdfMap.class);
 
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
