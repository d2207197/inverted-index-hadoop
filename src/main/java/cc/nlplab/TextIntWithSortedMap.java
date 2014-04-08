package cc.nlplab;


import cc.nlplab.TextIntWC;
import cc.nlplab.SortedMapW;
import cc.nlplab.PairW;


public class TextIntWithSortedMap extends PairW<TextIntWC, SortedMapW> {
  public TextIntWithSortedMap()
  {
    super(TextIntWC.class, SortedMapW.class);
  }
  public TextIntWithSortedMap(TextIntWC first, SortedMapW second)
  {
    super(TextIntWC.class, SortedMapW.class, first, second );
  }
}
