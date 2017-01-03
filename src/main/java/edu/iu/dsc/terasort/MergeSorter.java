package edu.iu.dsc.terasort;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MergeSorter {
  private static Logger LOG = Logger.getLogger(MergeSorter.class.getName());

  private Record[][] records;

  private List<Record[]> recordsList = new ArrayList<>();

  private static byte[] zero = new byte[Record.KEY_SIZE];
  private static byte[] largest = new byte[Record.KEY_SIZE];
  static {
    Arrays.fill( zero, (byte) 0 );
    Arrays.fill( largest, (byte) 255 );
  }

  public void addData(int rank, byte[] data) {
    LOG.log(Level.INFO, "Rank: " + rank + " receiving: " + data.length);
    // for now lets get the keys and sort them
    int size = data.length / Record.RECORD_LENGTH;
    Record[] r = new Record[size];
    byte[] key = new byte[Record.KEY_SIZE];
    byte[] text = new byte[Record.DATA_SIZE];
    for (int i = 0; i < size; i++) {
      System.arraycopy(data, i * Record.RECORD_LENGTH, key, 0, Record.KEY_SIZE);
      System.arraycopy(data, i * Record.RECORD_LENGTH + Record.KEY_SIZE, text, 0, Record.DATA_SIZE);
      r[i] = new Record(new Text(key), new Text(text));
    }
    records[rank] = r;
  }

  public void addData(ByteBuffer data, int size) {
    // for now lets get the keys and sort them
    int records = size / Record.RECORD_LENGTH;
    Record[] r = new Record[records];
    byte[] key = new byte[Record.KEY_SIZE];
    byte[] text = new byte[Record.DATA_SIZE];
    data.rewind();
    for (int i = 0; i < records; i++) {
      data.get(key, 0, Record.KEY_SIZE);
      data.get(text, 0, Record.DATA_SIZE);
      r[i] = new Record(new Text(key), new Text(text));
    }
    // sort the list and add
    LOG.info("Start sorting");
    Arrays.sort(r);
    recordsList.add(r);
    LOG.info("Done sorting");
  }

  public Record[] sort() {
    if (recordsList.size() == 0) {
      return merge(records, size);
    } else {
      LOG.info("Start merging");
      Record[][] toSort = new Record[recordsList.size()][];
      for (int i = 0; i < recordsList.size(); i++) {
        toSort[i] = recordsList.get(i);
      }
      Record[] merge = merge(toSort, toSort.length);
      LOG.info("Done merging");
      return merge;
    }
  }

  // Every Node will store the data and the list no from which it belongs
  class HeapNode {
    Record data;
    int listNo;

    public HeapNode(Record data, int listNo) {
      this.data = data;
      this.listNo = listNo;
    }
  }

  public int size;
  public HeapNode[] Heap;
  public int position;
  Record[] result;

  public MergeSorter() {

  }

  public Record[] merge(Record[][] A, int k) {
    records = new Record[k][];
    this.size = k;
    Heap = new HeapNode[k + 1]; // size + 1 because index 0 will be empty
    position = 0;
    Heap[0] = new HeapNode(new Record(new Text(zero)), -1); // put some junk values at 0th index node

    int nk = 0;
    for (int i = 0; i < A.length; i++) {
      nk += A[i].length;
    }
    result = new Record[nk];
    int count = 0;
    int[] ptrs = new int[k];
    // create index pointer for every list.
    for (int i = 0; i < ptrs.length; i++) {
      ptrs[i] = 0;
    }
    for (int i = 0; i < k; i++) {
      if (ptrs[i] < A[i].length) {
        insert(A[i][ptrs[i]], i); // insert the element into heap
      } else {
        insert(new Record(new Text(largest)), i); // if any of this list burns out, insert +infinity
      }

    }
    while (count < nk) {
      HeapNode h = extractMin(); // get the min node from the heap.
      result[count] = h.data; // store node data into result array
      ptrs[h.listNo]++; // increase the particular list pointer
      if (ptrs[h.listNo] < A[h.listNo].length) { // check if list is not burns out
        insert(A[h.listNo][ptrs[h.listNo]], h.listNo); // insert the next element from the list
      } else {
        insert(new Record(new Text(largest)), h.listNo); // if any of this list burns out, insert +infinity
      }
      count++;
    }
    return result;
  }

  public void insert(Record data, int listNo) {
    if (position == 0) { // check if Heap is empty
      Heap[position + 1] = new HeapNode(data, listNo); // insert the first element in heap
      position = 2;
    } else {
      Heap[position++] = new HeapNode(data, listNo);// insert the element to the end
      bubbleUp(); // call the bubble up operation
    }
  }

  public HeapNode extractMin() {
    HeapNode min = Heap[1]; // extract the root
    Heap[1] = Heap[position - 1]; // replace the root with the last element in the heap
    Heap[position - 1] = null; // set the last Node as NULL
    position--; // reduce the position pointer
    sinkDown(1); // sink down the root to its correct position
    return min;
  }

  public void sinkDown(int k) {
    int smallest = k;
    // check which is smaller child , 2k or 2k+1.
    if (2 * k < position && Heap[smallest].data.compareTo(Heap[2 * k].data) > 0)  {
      smallest = 2 * k;
    }
    if (2 * k + 1 < position && Heap[smallest].data.compareTo(Heap[2 * k + 1].data) > 0) {
      smallest = 2 * k + 1;
    }
    if (smallest != k) { // if any if the child is small, swap
      swap(k, smallest);
      sinkDown(smallest); // call recursively
    }
  }

  public void swap(int a, int b) {
    // System.out.println("swappinh" + mH[a] + " and " + mH[b]);
    HeapNode temp = Heap[a];
    Heap[a] = Heap[b];
    Heap[b] = temp;
  }

  public void bubbleUp() {
    int pos = position - 1; // last position
    while (pos > 0 && Heap[pos / 2].data.compareTo(Heap[pos].data) > 0) { // check if its parent is greater.
      HeapNode y = Heap[pos]; // if yes, then swap
      Heap[pos] = Heap[pos / 2];
      Heap[pos / 2] = y;
      pos = pos / 2; // make pos to its parent for next iteration.
    }
  }
}
