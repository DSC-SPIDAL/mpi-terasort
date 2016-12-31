package edu.iu.dsc.terasort;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MergeSorter {
  private static Logger LOG = Logger.getLogger(MergeSorter.class.getName());

  private List<Record> records = new ArrayList<>();

  public void addData(int rank, byte[] data) {
    LOG.log(Level.INFO, "Rank: " + rank + " receiving: " + data.length);
    // for now lets get the keys and sort them
    int size = data.length / Record.RECORD_LENGTH;
    for (int i = 0; i < size; i++) {
      byte[] key = new byte[Record.KEY_SIZE];
      byte[] text = new byte[Record.DATA_SIZE];
      System.arraycopy(data, i * Record.RECORD_LENGTH, key, 0, Record.KEY_SIZE);
      System.arraycopy(data, i * Record.RECORD_LENGTH + Record.KEY_SIZE, text, 0, Record.DATA_SIZE);
      records.add(new Record(new Text(key), new Text(text)));
    }
  }

  public List<Record> sort() {
    Collections.sort(records);
    return records;
  }


}
