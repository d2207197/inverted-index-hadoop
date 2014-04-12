package cc.nlplab;

// import java.io.IOException;
import java.io.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;


import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Retrieval  extends Configured implements Tool {
    public static final int N = 44;

    public static double tfIdf(final double tf, final double df) {
        return tf * Math.log(N / df);
    }

    public static HashMap<String, Integer> parseQuery(final String query) {

        HashMap<String, Integer> queryMap = new HashMap<String, Integer>();

        int index = 0;
        for (String term : query.split(" +(AND +|OR +)?-?")) {
            queryMap.put(term, index++);
        }

        return queryMap;
    }

    public static class FilterQueryMapper
        extends Mapper<TextIntWC, TermInfoArray,
        NullWritable, TermDfTermInfoArray> {
        private static String query;
        private static HashMap<String, Integer> queryMap;
        private Logger log = Logger.getLogger(FilterQueryMapper.class);

        @Override
        protected final void setup(final Mapper.Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            queryMap = parseQuery(query);
        }

        public final void map(TextIntWC termDf, TermInfoArray termInfos
                        , Context context)
            throws IOException, InterruptedException {
            String termString = termDf.getFirst().toString();
            int df = termDf.getSecond().get();
            if (queryMap.containsKey(termString))
                context.write(NullWritable.get(), new TermDfTermInfoArray( termString, df, termInfos));
        }
    }


    public static class RetrievalReducer extends Reducer<NullWritable, TermDfTermInfoArray, NullWritable, FileInfoArray> {
        private static String query;
        private static HashMap<String, Integer> queryMap ;
        Logger log = Logger.getLogger(RetrievalReducer.class);

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            // queryMap = parseQuery(query);
        }

        public void reduce (NullWritable none, Iterable<TermDfTermInfoArray> termDfTermInfos_iter, Context context) throws IOException, InterruptedException {
            QueryHandler queryhandler = new QueryHandler();

            for (TermDfTermInfoArray termDfTermInfos: termDfTermInfos_iter ) {
                String term = termDfTermInfos.getTerm();
                int df = termDfTermInfos.getDf();
                ArrayWritable termInfos = termDfTermInfos.getTermInfos();

                ArrayList<FileInfo> fileInfos = new ArrayList<FileInfo>();



                for (Writable entry : termInfos.get()) {
                    TermInfo termInfo = (TermInfo) entry;
                    String fileName = termInfo.getFileName();
                    int tf = termInfo.getTf();
                    LongArrayListW offsets = termInfo.getOffsets();

                    ArrayList<TermOffsets> termOffsets = new ArrayList<TermOffsets>();
                    termOffsets.add(new TermOffsets(term, offsets));
                    fileInfos.add(new FileInfo(fileName, (tfIdf(tf, df)),  termOffsets));
                }

                queryhandler.putFileInfoArray(term, new FileInfoArray(fileInfos));
            }
            System.out.println("Start parsing: " + query );
            FileInfoArray output = queryhandler.parser.parse(query);
            context.write(NullWritable.get(), output);
        }

    }

    private final Logger log = Logger.getLogger(getClass());
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("query", args[2]);

        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path docPath = new Path(args[1]);
        Path parentPath = inputPath.getParent();
        Path outputPath = new Path(parentPath, getClass().getSimpleName());
        fs.delete(outputPath);

        Job job = new Job(getConf(), "Retrieval");

        job.setJarByClass(getClass());


        // input
        SequenceFileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // map
        job.setMapperClass(FilterQueryMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TermDfTermInfoArray.class);

        // reducer
        job.setReducerClass(RetrievalReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FileInfoArray.class);

        job.setNumReduceTasks(1);
        // output
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        Path pt=new Path( outputPath, "part-r-00000");
        // FileSystem fs = FileSystem.get(getConf());
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pt, getConf());
        // BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));


        FileInfoArray fInfos = new FileInfoArray();


        while(reader.next(NullWritable.get(),  fInfos)) {
            FileInfo [] fileInfos = (FileInfo [])fInfos.toArray();
            Arrays.sort(fileInfos, Collections.reverseOrder());
            int i = 0;
            for (FileInfo fileInfo : fileInfos) {
                if ( i == 10)
                    break;
                System.out.println("---------------------------------------------------------------");
                System.out.printf("%-20s%40g\n", fileInfo.getFileName() ,fileInfo.getScore());
                for (TermOffsets tmOfs: fileInfo.getTermOffsets()) {
                    System.out.println( "#" + tmOfs.getTerm() );
                    FSDataInputStream doc = fs.open(new Path(docPath, fileInfo.getFileName()));

                    int j = 0;
                    for (Long offset: tmOfs.getOffsets()) {
                        if (j >= 5) break;
                        long start ;
                        long end;
                        byte buffer[] = new byte[40];

                        if(offset <= 20) {
                            start = 0;
                            end = 40;
                            // mark_start = offset;
                            // mark_end = offset + tmOfs.getTerm().length();
                        } else {
                            start = offset - 20;
                            end = offset + 20;
                            // pos = 20;
                            // mark_end = 20 + tmOfs.getTerm().length();
                        }


                        doc.read(start, buffer, 0, 40);
                        String sent = new String(Arrays.copyOfRange(buffer, 0, 20), "UTF-8").replaceAll("\n", " ")
                        + "\033[1;32m" + tmOfs.getTerm() + "\033[0m"
                        + new String(Arrays.copyOfRange(buffer, 20+tmOfs.getTerm().length(), 40), "UTF-8").replaceAll("\n", "  ");

                        System.out.println( "     " + offset + ": " + sent);
                        j++;
                    }
                    // System.out.println();
                }
                i++;

            }

        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Retrieval(), args);
        System.exit(exitCode);
    }

}
