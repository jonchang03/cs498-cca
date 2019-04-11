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

/** a bolt that finds the top n words. */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;

  // Hint: Add necessary instance variables and inner classes if needed
  private int N;
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  // public TopNFinderBolt withNProperties(int N) {
  //   /* ----------------------TODO-----------------------
  //   Task: set N
  //   ------------------------------------------------- */

	// 	// End
	// 	return this;
  // }

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
    currentTopWords.put(word, count);
    if (currentTopWords.size() > N){
        Collection<Integer> values = currentTopWords.values();
        values.remove(Collections.min(values));
    }
  
    collector.emit(new Values("top-N", printMap()));

		// End
  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    
    // stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      // stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
      stringBuilder.append(word + ", ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    // stringBuilder.append("]");
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
