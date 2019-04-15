package edu.illinois.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import edu.illinois.storm.Pair;

/** a bolt that finds the top n words. */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;

  // Hint: Add necessary instance variables and inner classes if needed
  private int N = 10;
  private long intervalToReport = 20; 
  private long lastReportTime = System.currentTimeMillis();

  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  // private HashMap<String, Integer> map = new HashMap<String, Integer>();

  private PriorityQueue<Pair> queue = new PriorityQueue<Pair>();


  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple) {
    /* ----------------------TODO-----------------------
    Task: keep track of the top N words
		Hint: implement efficient algorithm so that it won't be shutdown before task finished
		      the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
    ------------------------------------------------- */
    String word = tuple.getString(0);
    Integer count = Integer.parseInt(tuple.getString(1));
    
    // if(map.containsKey(word)){
    //   if(count > map.get(word)) {
    //     map.put(word, count);
    //   }
    // } else{
    //   map.put(word, count);
    // }
    

    //  maintain a heap of size N. 
    Pair p = new Pair(word, count);
  
    // need to make sure key doesn't already exist
    // maintain set of words

    if (queue.size() <= N) {
      // if word isn't in current top 10
      if (!currentTopWords.containsKey(word)){
        queue.add(p);
        currentTopWords.put(word, count);
      } 
      // if word is in current top 10, and higher count is received
      else if (count > currentTopWords.get(word)) {
        currentTopWords.put(word, count);
      } else { // otherwise we'll keep the original count
        count = currentTopWords.get(word);
      }
    }

    // remove minimum to make way for new entry
    else if (queue.peek().getValue() < count) {
      // remove entry from hash map by value
      currentTopWords.remove(queue.peek().getKey());
      if (!currentTopWords.containsKey(word)){
        queue.poll();
        queue.add(p);
      }
      currentTopWords.put(word, count);
    } 

 
    //get all elements from the heap
    //  List<Integer> result = new ArrayList<>();
    // currentTopWords.clear();

    // PriorityQueue<Pair> queue_copy = new PriorityQueue<Pair>(queue);
    // while(queue_copy.size() > 0){
    //     currentTopWords.put(queue_copy.peek().getKey(), queue_copy.peek().getValue()); // can't poll both
    //     queue_copy.poll();
    // }


  

    // reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values("top-N", printMap()));
      lastReportTime = System.currentTimeMillis();
    }
    // collector.emit(new Values("top-N", printMap()));

		// End
  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
        stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define output fields
		Hint: there's no requirement on sequence;
					For example, for top 3 words set ("hello", "word", "cs498"),
					"hello, world, cs498" and "world, cs498, hello" are all correct
    ------------------------------------------------- */
    declarer.declare(new Fields("top-N", "top-N-words"));
    // END
  }
}

// class Pair
// {
//   String word;
//   int count;
//   // Constructor
//   public Pair(String word, int count){
//       this.word=word;
//       this.count=count;
//   }
// }


// // Comparator to compare Strings 
// // https://www.geeksforgeeks.org/priorityblockingqueue-comparator-method-in-java/
// class COMPARING implements Comparator<Pair> { 
//   @Override
//   public int compare(Pair p1, Pair p2) 
//   { 
//       // Compare value by frequency 
//       int freqCompare = Integer.compare(p2.count, p1.count); 

//       // Compare value if frequency is equal 
//       int valueCompare = p1.word.compareTo(p2.word); 

//       // If frequency is equal, then just compare by value, otherwise - 
//       // compare by the frequency. 
//       if (freqCompare == 0) 
//           return valueCompare; 
//       else
//           return freqCompare; 
//   } 
// }