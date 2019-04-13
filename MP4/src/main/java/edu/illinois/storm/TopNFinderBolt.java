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

/** a bolt that finds the top n words. */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;

  // Hint: Add necessary instance variables and inner classes if needed
  private int N;
  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();

  private ConcurrentMap<String, Integer> map =  new ConcurrentHashMap<String, Integer>();
  
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

    Integer val = map.get(word);
    if(val == null){
        map.put(word, count); 
    }
    else{
        map.put(word, val + count);
    }
    
    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values("top-N", printMap()));
      lastReportTime = System.currentTimeMillis();
    }
    // collector.emit(new Values("top-N", printMap()));

		// End
  }
  public String printMap() {
    StringBuffer sb = new StringBuffer();

    // Vector<Entry<String, Integer>> vec = new Vector<Entry<String, Integer>>(map.entrySet());
    // Collections.sort(vec, new Comparator<Entry<String, Integer>>(){
    //     public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2){
    //       // Compare value by frequency 
    //       int freqCompare = o1.getValue().compareTo(o2.getValue());
    
    //       // Compare value if frequency is equal 
    //       int valueCompare = o1.getKey().compareTo(o2.getKey()); 
    
    //       // If frequency is equal, then just compare by value, otherwise - 
    //       // compare by the frequency. 
    //       if (freqCompare == 0) 
    //           return valueCompare; 
    //       else
    //           return freqCompare; 
    //     }
    // });
    // int left = Math.max(vec.size() - N, 0);
    // for(int i = left ; i < vec.size(); i++){
    //     sb.append(vec.get(i).getKey() + ", ");
    // }
    Map<String, Integer> sortedMap = map.entrySet().stream()
      .sorted(Map.Entry.<String, Integer>comparingByValue().reversed()
              .thenComparing(Map.Entry.<String, Integer>comparingByKey().reversed()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
              (e1, e2) -> e1, LinkedHashMap::new));

    Set<String> keys = sortedMap.keySet();
    String[] keysArray = keys.toArray(new String[keys.size()]);
    for(int i=0; i<keysArray.length && i<N; i++) {
        sb.append(keysArray[i] + ", ");
    }



    int lastCommaIndex = sb.lastIndexOf(",");
    sb.deleteCharAt(lastCommaIndex + 1);
    sb.deleteCharAt(lastCommaIndex);
    return sb.toString();
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
