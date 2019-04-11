package edu.illinois.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/** a spout that generate sentences from a file */
public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext _context;
  private String inputFile;

  // Hint: Add necessary instance variables if needed
  private BufferedReader reader;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this._context = context;
    this._collector = collector;

    /* ----------------------TODO-----------------------
    Task: initialize the file reader
    ------------------------------------------------- */
    try {
      reader = new BufferedReader(new FileReader(inputFile));
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    }
    // END

  }

  // Set input file path
  // public FileReaderSpout withInputFileProperties(String inputFile) {
  //   this.inputFile = inputFile;
  //   return this;
  // }
  public void setInputFile(String inputFile) {
    this.inputFile = inputFile;
  }

  @Override
  public void nextTuple() {

    /* ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to add a small sleep when the file is entirely read to prevent a busy-loop
    ------------------------------------------------- */
    try{
      while(reader.ready()) {
          String line = reader.readLine();
          _collector.emit(new Values(line));
      }
    } catch(IOException e){
        e.printStackTrace();
    }
    Utils.sleep(1000);
    // END
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define the declarer
    ------------------------------------------------- */
    declarer.declare(new Fields("word"));
    // END
  }

  @Override
  public void close() {
    /* ----------------------TODO-----------------------
    Task: close the file
    ------------------------------------------------- */
    try {
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // END

  }

  public void fail(Object msgId) {}

  public void ack(Object msgId) {}

  @Override
  public void activate() {}

  @Override
  public void deactivate() {}

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
