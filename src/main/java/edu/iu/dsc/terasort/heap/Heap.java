package edu.iu.dsc.terasort.heap;

import edu.iu.dsc.terasort.Record;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

public class Heap {
  public static byte[] zero = new byte[Record.KEY_SIZE];
  public static byte[] largest = new byte[Record.KEY_SIZE];
  static {
    Arrays.fill( zero, (byte) 0 );
    Arrays.fill( largest, (byte) 255 );
  }

  public int size;
  public HeapNode[] heap;
  public int position;

  public Heap(int k) {
    this.size = k;
    // size + 1 because index 0 will be empty
    heap = new HeapNode[k + 1];
    position = 0;
    // put some junk values at 0th index node
    heap[0] = new HeapNode(new Record(new Text(zero)), -1);
  }

  public void insert(Record data, int listNo) {
    // check if Heap is empty
    if (position == 0) {
      // insert the first element in heap
      heap[position + 1] = new HeapNode(data, listNo);
      position = 2;
    } else {
      // insert the element to the end
      heap[position++] = new HeapNode(data, listNo);
      bubbleUp();
    }
  }

  public HeapNode extractMin() {
    // extract the root
    HeapNode min = heap[1];
    // replace the root with the last element in the heap
    heap[1] = heap[position - 1];
    // set the last Node as NULL
    heap[position - 1] = null;
    // reduce the position pointer
    position--;
    // sink down the root to its correct position
    sinkDown(1);
    return min;
  }

  public void sinkDown(int k) {
    int smallest = k;
    // check which is smaller child , 2k or 2k+1.
    if (2 * k < position && heap[smallest].data.compareTo(heap[2 * k].data) > 0)  {
      smallest = 2 * k;
    }
    if (2 * k + 1 < position && heap[smallest].data.compareTo(heap[2 * k + 1].data) > 0) {
      smallest = 2 * k + 1;
    }
    if (smallest != k) { // if any if the child is small, swap
      swap(k, smallest);
      sinkDown(smallest); // call recursively
    }
  }

  public void swap(int a, int b) {
    // System.out.println("swappinh" + mH[a] + " and " + mH[b]);
    HeapNode temp = heap[a];
    heap[a] = heap[b];
    heap[b] = temp;
  }

  public void bubbleUp() {
    // last position
    int pos = position - 1;
    // check if its parent is greater.
    while (pos > 0 && heap[pos / 2].data.compareTo(heap[pos].data) > 0) {
      HeapNode y = heap[pos]; // if yes, then swap
      heap[pos] = heap[pos / 2];
      heap[pos / 2] = y;
      pos = pos / 2; // make pos to its parent for next iteration.
    }
  }
}
