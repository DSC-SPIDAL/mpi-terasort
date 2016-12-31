package edu.iu.dsc.stats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StatCalculator {
  public Pair<Long, Double> calculate(String fileName) {
    Path file = Paths.get(fileName);
    try (InputStream in = Files.newInputStream(file);
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(in))) {
      String line = null;
      long numbers = 0;
      double sum = 0;
      while ((line = reader.readLine()) != null) {
        String []split = line.split(",");
        for (String n : split) {
          sum += Double.valueOf(n);
          numbers++;
        }
      }
      return new Pair<>(numbers, sum);
    } catch (IOException x) {
      x.printStackTrace();
    }
    return new Pair<Long, Double>(0L, 0.0);
  }
}
