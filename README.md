build-inverted-index-hadoop
===========================


## Instruction (Short version) ##

    $ mvn clean compile assembly:single
    $ hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cc.nlplab.BuildInvertedIndex  /opt/HW1/input1 inverted-index-output
    $ hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cc.nlplab.Query inverted-index-output /opt/HW1/input1 "book OR sport store -went twenty"

## Instruction (Long version) ##

### Install Maven ###

The project should be built with Maven. If the system haven't install Maven. Install it with the following command in Debian/Ubuntu distros.

    $ sudo apt-get install maven2

### Build the jar file ###

After Maven have been installed. Project can be built with following command.

    $ mvn clean compile assembly:single

The jar file should be generated in the *target/* folder.

    $ ls target/
    archive-tmp  classes  generated-sources  inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar  maven-status

### Generate inverted index ###

Use `BuildInvertedIndex` class for generating inverted index files. The command line syntax of the class as shown in the following.

    $ hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cc.nlplab.BuildInvertedIndex -help
    usage: BuildInvertedIndex [OPTION]... <INPUTPATH> <OUTPUTPATH>
    Process text files in INPUTPATH and build inverted index to OUTPUTPATH.
    
     -help   this help message.
     -text   output in text format(only for checking)

By default, BuildInvertedIndex class would generate inverted index in SequenceFile format. If you want text format, add `-text` option. The text format output is only for human checking, can't be query with the Query class.

To generate the inverted index files of the documents in `/opt/HW1/input1` to `inverted-index-output` folder.

    $ hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cc.nlplab.BuildInvertedIndex  /opt/HW1/input1 inverted-index-output

### Query the inverted index ###

After generated inverted index files to **inverted-index-output** folder, we can use `Query` class for querying the inverted index. 

The command line syntax of `Query` class:

    $ hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cc.nlplab.Query -help
    usage: Query [OPTION]... <INVERTED_INDEX_PATH> <DOC_PATH> <QUERY>
    Search words of documents in <doc_path> based on query and calculated
    inverted_inedx.

     -help   this help message.

- The *INVERTED_INDEX_PATH* is the output of `BuildInvertedIndex` class.
- The *DOC_PATH* is the input of `BuildInvertedIndex` class.
- The *QUERY* is following basic Google query syntax.
    - `term1 term2` means *AND*
    - `term1 OR term2` means *OR*
    - `-term1` means no term1


To query the inverted index files in `inverted-index-output` built from documents in `/opt/HW1/input1`.

    $ hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cc.nlplab.Query inverted-index-outpu /opt/HW1/input1 "book OR sport store -went twenty"
    ---------------------------------------------------------------
    titusandronicus                                      2.69564
    #sport
         32879: s have,     And to our sport.       [To TAMORA]
         36947: intercepted in your sport,          Great reason
         ...
    #store
         5878: f mine hast thou in store,   That thou wi
    #twenty
         5197: Romans, of five and twenty valiant sons,
         ...
    ---------------------------------------------------------------
    loveslabourslost                                     2.55864
    #book
         4078: ully to pore upon a book     To seek the li
    ...
    ---------------------------------------------------------------
    coriolanus                                           2.25117
    #book
         89214: ll'd        In Jove's own book, like an unnatu
    ...


The output is in the following format. Top 10 high score documents, all matched terms of each documents, first five offsets of each term and words around the offset in the document.

    ---------------------------------------------------------------
    <document name>                                      <score>
    #<term>
         <offset>: <words in the documents>
         <offset>: <words in the documents>
         ...
    #<term>
         ...
    ---------------------------------------------------------------



## Design ##

### BuildInvertedIndex ###

#### 1. Mapper ####

Split terms in line and generate one record for each term. The *filename* in key makes sure filenames the reducer recieved is sorted.

Input: *offset* => *line*

    238 => aaa bbb aaa
    445 => bbb aaa bbb

Output: *term*, *filename* => *filename*, *1*, [*offset* + *offset_in_line*]

    aaa, file1 => file1, 1, [238]
    bbb, file1 => file1, 1, [242]
    aaa, file1 => file1, 1, [246]
    bbb, file2 => file2, 1, [445]
    aaa, file2 => file2, 1, [449]
    bbb, file2 => file2, 1, [453]

#### 2. Combiner ####

Combine all record with the same term and filename. In this step, the *term_frequency* is summerized and *offset*s is merged into a list.

Input: *term*, *filename* => [(*filename*, *1*, [*offset* + *offset_in_line*]), ...]

    aaa, file1 => [ file1, 1, [238],
                    file1, 1, [246] ]
    aaa, file2 => [ file2, 1, [449] ]
    bbb, file1 => [ file1, 1, [242] ]
    bbb, file2 => [ file2, 1, [445],
                    file2, 1, [453] ]


Output: *term*, *filename* => *filename*, *term_frequnecy*, [*offset*, ...]

    aaa, file1 => file1, 2, [238, 246]
    aaa, file2 => file2, 1, [449]
    bbb, file1 => file1, 1, [242]
    bbb, file2 => file2, 2, [445, 453]

#### 3. Partitioner ####

If the number of reducers is more than one, record with the same *term* might be sent to different reducer. The partitioner makes sure all record with the same *term* would be sent to the same reducer. 

Input: *term*, *filename* => *filename*, *term_frequnecy*, [*offset*, ...]

    aaa, file1 => file1, 2, [238, 246]
    aaa, file2 => file2, 1, [449]
    bbb, file1 => file1, 1, [242]
    bbb, file2 => file2, 2, [445, 453]

Output:

    hashCode(aaa)
    hashCode(aaa)
    hashCode(bbb)
    hashCode(bbb)

#### 4. GroupComparator ####

Because the key is composed of both *term* and *filename*, records would be sorted by both *term* and *filename*. But in the reducer, records should be grouped by only *term* for calculating document frequencies. This group comparator ignore *filename* and compares only *term*s that makes sure records would be grouped by *term* only. 


Input: (*term*, *filename*), (*term*, *filename*)

    (aaa, file1), (aaa, file1)
    (aaa, file1), (bbb, file2)
    (bbb, file1), (bbb, file2)
    ...

Output:

    "aaa".compareTo("aaa")
    "aaa".compareTo("bbb")
    "bbb".compareTo("bbb")
    ...


#### 5. Reducer ####

The reducer merged values into a list and count the number of values.

Input: *term*, *filename* => [(*filename*, *term_frequnecy*, [*offset*, ...]), ...]

    aaa, file1 => file1, 2, [238, 246]
                  file2, 1, [449]
    bbb, file1 => file1, 1, [242]
                  file2, 2, [445, 453]

Output: *term*, *document_frequency* => [(*filename*, *term_frequnecy*, [*offset*, ...]), ...]


    aaa, 2 => [(file1, 2, [238, 246]), (file2, 1, [449])]
    bbb, 2 => [(file1, 1, [242]), (file2, 2, [445, 453])]


By default, the output of `BuildInvertedIndex` is in `SequenceFile` format which can be easily restore to java objects.


### Query ###


Steps in `Query` class:

1. Reads records directly from HDFS
2. Filter records by terms in the given query
3. Generate file list of each term and calculate tf-idf score of each file
4. Merge file list according to query
5. Print the merged file list


## Questions ##

### 1. How many pass do you used to run mapReduce in part1? Is there any other method to do it? What's the advantage and disadvantage? ###

One pass mapReduce in part1.

Many other ways:

1. Term frequencies can be calculated in the mapper not combiner.
   Advantage: No combiner.
   Disadvantage: The mapper needs to keep a big variable for storing the counts and offsets of all kinds of words. Consumes more memory.

2. Term frequencies can be calculated in the reducer.
   Disadvantage: That needs one more map reduce pass for calculating document frequency.

3. File names can be sorted in the reducer.
   Advantage: The redundant file name in the key can be removed.
   Disadvantage: That needs more memory for sorting file names.

### 2. What is your extension? What’s the most difficult part in your implementation? ###

My extension is supporting basic Google query syntax. All supported operands is described in the following.

- `term1 term2` means *AND*
- `term1 OR term2` means *OR*
- `-term1` means no term1

The query `universe -wood book OR sport store`  means `(universe AND (book OR sport) AND store) WITHOUT wood`.
The most difficult part is to use **jparsec** library to parse the query and merge the file list of each term by the syntax of query. This is my first java program and the **jparsec** library is pretty complex.
