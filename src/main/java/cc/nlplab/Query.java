package cc.nlplab;

import java.util.HashMap;
import java.util.Collections;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.cli.*;



public class Query  extends Configured implements Tool {
    public static final int N = 44;

    public static HashMap<String, Integer> parseQuery(final String query) {

        HashMap<String, Integer> queryMap = new HashMap<String, Integer>();

        int index = 0;
        for (String term : query.split(" +(AND +|OR +)?-?")) {
            queryMap.put(term, index++);
        }

        return queryMap;
    }

    public static double tfIdf(final double tf, final double df) {
        return tf * Math.log(N / df);
    }

    public static void printResult(FileInfo [] resultArray, Path docPath, FileSystem fs)
        throws IOException, UnsupportedEncodingException {
        int i = 0;
        for (FileInfo fileInfo : resultArray) {
            
            if ( i == 10) break;
            
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


    public void printHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "Query [OPTION]... <INVERTED_INDEX_PATH> <DOC_PATH> <QUERY>", "Search words of documents in <doc_path> based on query and calculated inverted_inedx.\n", options, "");
    }


    private CommandLine argsParse(String [] args) {

        Options options = new Options();
        
        options.addOption("help", false, "this help message.");

        CommandLineParser parser = new GnuParser();
        CommandLine cmd ;
        try {
            cmd = parser.parse( options, args);
        }
        catch( ParseException exp ) {
            System.err.println( "Unexpected exception:" + exp.getMessage() );
            printHelp(options);

            return null;
        }
        if (cmd.hasOption("help") || cmd.getArgs().length < 3) {
            printHelp(options);
            return null;
        }
        return cmd;

    }


    public int run(String[] args) throws Exception {

        CommandLine cmd = argsParse(args);
        if (cmd == null) {
            return 1;
        }
        
        String query = cmd.getArgs()[2];
        HashMap<String, Integer> queryMap = parseQuery(query);
        QueryHandler queryhandler = new QueryHandler();
        
        FileSystem fs = FileSystem.get(getConf());
        Path inputPath = new Path(cmd.getArgs()[0]);
        Path docPath = new Path(cmd.getArgs()[1]);
        Path parentPath = inputPath.getParent();

        FileStatus[] status = fs.listStatus(inputPath);
        for (int i=0;i<status.length;i++){

            if (status[i].isDir() )
                continue;
            SequenceFile.Reader reader;
            try {
                reader = new SequenceFile.Reader(fs, status[i].getPath(), getConf());
            }
            catch (java.io.EOFException e) {
                continue;
            }
            System.out.println("reading" + status[i].getPath());

            TextIntWC termDf = new TextIntWC();
            TermInfoArray termInfos = new TermInfoArray();
        

            while(reader.next(termDf, termInfos)) {
                String term = termDf.getFirst().toString();
                int df = termDf.getSecond().get();
                if (queryMap.containsKey(term)) {
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
            }
        }
        System.out.println("Start parsing: " + query );
        FileInfoArray result = queryhandler.parser.parse(query);
        System.out.println(result);

        FileInfo [] resultArray = (FileInfo [])result.toArray();
        Arrays.sort(resultArray, Collections.reverseOrder());
        
        printResult(resultArray, docPath, fs);

        return 0;

    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Query(), args);
        System.exit(exitCode);
    }
}
