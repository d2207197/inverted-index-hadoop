package cc.nlplab;
import cc.nlplab.FileTfIdfMap;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.functors.*;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.ArrayList;


public class QueryHandler {
  
  enum BinaryOperator implements Binary<FileTfIdfMap> {
    AND {
      public FileTfIdfMap map(FileTfIdfMap a, FileTfIdfMap b) {
	System.out.print(a + " AND " + b + " = ");
	FileTfIdfMap out = a.and(b);
	System.out.println(out);
        return out;
      }
    },
    OR {
      public FileTfIdfMap map(FileTfIdfMap a, FileTfIdfMap b) {
	System.out.print(a + " OR " + b + " = ");
	FileTfIdfMap out = a.or(b);
	System.out.println(out);
	return out;
      }
    }
  }
  
  enum UnaryOperator implements Unary<FileTfIdfMap> {
    NEG {
      public FileTfIdfMap map(FileTfIdfMap n) {
        return n.not();
	
      }
    }
  }

  private HashMap<String, FileTfIdfMap> fileTfIdfMaps = new HashMap<String, FileTfIdfMap>();
  public void putfileTfIdfMap(String fileName, FileTfIdfMap fileTfIdfMap)
  {
    this.fileTfIdfMaps.put(fileName, fileTfIdfMap);
  }
  
  
  final Parser<FileTfIdfMap> IDENTIFIER = Terminals.Identifier.PARSER.map(new Map<String, FileTfIdfMap>() {
      public FileTfIdfMap map(String s) {
		System.out.println(" get FileTfIdfMap: " + s);
		System.out.println(fileTfIdfMaps.get(s) );

        return fileTfIdfMaps.get(s);
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
  
  static Parser<FileTfIdfMap> queryHandler(Parser<FileTfIdfMap> atom) {
    Parser<FileTfIdfMap> parser = new OperatorTable<FileTfIdfMap>()
    .prefix(op("-", UnaryOperator.NEG), 30)
    .infixl(op("AND", BinaryOperator.AND).or(WHITESPACE_AND), 10)
    .infixl(op("OR", BinaryOperator.OR), 20)
    .build(atom);
    return parser;
  }
  
  public final Parser<FileTfIdfMap> parser = queryHandler(IDENTIFIER).from(TOKENIZER, IGNORED);


  // public static void main(String [] args)
  // {
  //   FileTfIdfMap aaa = new FileTfIdfMap();
  //   aaa.put("file1", 1.1);
  //   aaa.put("file2", 1.2);
  //   aaa.put("file3", 1.3);
  //   FileTfIdfMap bbb = new FileTfIdfMap();
  //   bbb.put("file2", 1.1);
  //   bbb.put("file3", 1.2);
  //   bbb.put("file4", 1.3);

  //   FileTfIdfMap ccc = new FileTfIdfMap();
  //   ccc.put("file1", 1.1);
  //   ccc.put("file2", 1.2);
  //   FileTfIdfMap ddd = new FileTfIdfMap();
  //   ddd.put("file3", 1.2);
  //   ddd.put("file4", 1.3);


  //   fileTfIdfMaps.put("aaa", aaa);
  //   fileTfIdfMaps.put("bbb", bbb);
  //   fileTfIdfMaps.put("ccc", ccc);
  //   fileTfIdfMaps.put("ddd", ddd);
    
  //   System.out.println(CALCULATOR.parse("aaa bbb -ccc"));

  // }
  
}
