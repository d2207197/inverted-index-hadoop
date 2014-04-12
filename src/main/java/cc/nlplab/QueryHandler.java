package cc.nlplab;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.functors.*;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.ArrayList;


public class QueryHandler {

    enum BinaryOperator implements Binary<FileInfoArray> {
        AND {
            public FileInfoArray map(FileInfoArray a, FileInfoArray b) {
                System.out.println("\033[1;33mAND\033[m ");
                System.out.println("    \033[1;34m1:\033[m " + a);
                System.out.println("    \033[1;34m2:\033[m " + b);
                
                FileInfoArray out = a.and(b);
                System.out.println("    \033[1;34m=\033[m " + out + "\n");
                return out;
            }
        },
        OR {
            public FileInfoArray map(FileInfoArray a, FileInfoArray b) {
                System.out.println("\033[1;33mOR\033[m ");
                System.out.println("    \033[1;34m1:\033[m " + a);
                System.out.println("    \033[1;34m2:\033[m " + b);

                FileInfoArray out = a.or(b);
                System.out.println("    \033[1;34m=\033[m " + out + "\n");
                return out;
            }
        }
    }

    enum UnaryOperator implements Unary<FileInfoArray> {
        NEG {
            public FileInfoArray map(FileInfoArray n) {
                System.out.println("\033[1;33m-\033[m\n    \033[1;34m1:\033[m " + n + "\n" );
                return n.not();
            }
        }
    }

    private HashMap<String, FileInfoArray> fileInfoMaps = new HashMap<String, FileInfoArray>();
    public void putFileInfoArray(String term, FileInfoArray fileInfos) {
        this.fileInfoMaps.put(term, fileInfos);
    }

    final Parser<FileInfoArray> IDENTIFIER = Terminals.Identifier.PARSER.map(new Map<String, FileInfoArray>() {
        public FileInfoArray map(String s) {
            // System.out.println(" get FileInfoArray: " + s);
            // System.out.println(fileInfoMaps.get(s) );

            return fileInfoMaps.get(s);
        }
    });

    private static final Terminals OPERATORS = Terminals.operators("AND", "OR", "-", "\"");

    static final Parser<Void> IGNORED = Scanners.WHITESPACES.skipMany();

    static final Parser<?> TOKENIZER =
        Parsers.or( OPERATORS.tokenizer(), Terminals.Identifier.TOKENIZER);

    static Parser<?> term(String... names) {
        return OPERATORS.token(names);
    }

    static final Parser<BinaryOperator> WHITESPACE_AND =
        term("AND", "OR", "\"").not().retn(BinaryOperator.AND);

    static <T> Parser<T> op(String name, T value) {
        return term(name).retn(value);
    }

    static Parser<FileInfoArray> queryHandler(Parser<FileInfoArray> atom) {
        Parser<FileInfoArray> parser = new OperatorTable<FileInfoArray>()
        .prefix(op("-", UnaryOperator.NEG), 30)
        .infixl(op("AND", BinaryOperator.AND).or(WHITESPACE_AND), 10)
        .infixl(op("OR", BinaryOperator.OR), 20)
        .build(atom);
        return parser;
    }

    public final Parser<FileInfoArray> parser = queryHandler(IDENTIFIER).from(TOKENIZER, IGNORED);


    // public static void main(String [] args)
    // {
    //   FileInfoArray aaa = new FileInfoArray();
    //   aaa.put("file1", 1.1);
    //   aaa.put("file2", 1.2);
    //   aaa.put("file3", 1.3);
    //   FileInfoArray bbb = new FileInfoArray();
    //   bbb.put("file2", 1.1);
    //   bbb.put("file3", 1.2);
    //   bbb.put("file4", 1.3);

    //   FileInfoArray ccc = new FileInfoArray();
    //   ccc.put("file1", 1.1);
    //   ccc.put("file2", 1.2);
    //   FileInfoArray ddd = new FileInfoArray();
    //   ddd.put("file3", 1.2);
    //   ddd.put("file4", 1.3);


    //   fileTfIdfMaps.put("aaa", aaa);
    //   fileTfIdfMaps.put("bbb", bbb);
    //   fileTfIdfMaps.put("ccc", ccc);
    //   fileTfIdfMaps.put("ddd", ddd);

    //   System.out.println(CALCULATOR.parse("aaa bbb -ccc"));

    // }

}
