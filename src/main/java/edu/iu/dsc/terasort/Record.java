package edu.iu.dsc.terasort;

import org.apache.hadoop.io.Text;

public class Record implements Comparable<Record> {
  public static final int KEY_SIZE = 10;
  public static final int DATA_SIZE = 90;
  public static final int RECORD_LENGTH = KEY_SIZE + DATA_SIZE;

  private Text key;
  private Text text;

  public Record(Text key) {
    this.key = key;
  }

  public Record(Text key, Text text) {
    this.key = key;
    this.text = text;
  }

  public Text getKey() {
    return key;
  }

  public Text getText() {
    return text;
  }

  @Override
  public int compareTo(Record o) {
    return this.key.compareTo(o.getKey());
  }
}
