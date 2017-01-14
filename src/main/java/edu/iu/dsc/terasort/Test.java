package edu.iu.dsc.terasort;

import edu.iu.dsc.terasort.heap.Heap;
import org.apache.hadoop.io.Text;

/**
 * Created by supun on 1/14/17.
 */
public class Test {
  public static void main(String[] args) {
    Text t = new Text(Heap.largest);
    Text t2 = new Text(Heap.zero);
    System.out.println(t2.compareTo(t));
  }
}
