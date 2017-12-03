package edu.iu.dsc.stats;

import java.util.ArrayList;
import java.util.List;

public class StatProgram {
  public static void main(String[] args) {
    int partitionSize = 100;
    List<Double> numbers = new ArrayList<>();
    double sum = 0, average = 0;
    for (int i = 0; i < partitionSize; i++) {
      sum += numbers.get(0);
    }
    average = sum / partitionSize;
    
  }
}
