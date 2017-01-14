package edu.iu.dsc.terasort.heap;

import edu.iu.dsc.terasort.Record;

public class HeapNode {
  public Record data;
  public int listNo;

  public HeapNode(Record data, int listNo) {
    this.data = data;
    this.listNo = listNo;
  }
}
