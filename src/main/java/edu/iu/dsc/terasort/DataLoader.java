package edu.iu.dsc.terasort;

import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assume data is partitioned into file
 */
public class DataLoader {
  private static Logger LOG = Logger.getLogger(DataLoader.class.getName());

  private List<Record> records = new ArrayList<>();

  private String inFileName;
  private String outFileName;

  private byte[] buffer = new byte[Record.RECORD_LENGTH];

  public DataLoader(String inFileName, String outFileName) {
    this.inFileName = inFileName;
    this.outFileName = outFileName;
  }

  public List<Record> load() {
    try {
      DataInputStream in = new DataInputStream(
          new BufferedInputStream(
              new FileInputStream(new File(inFileName))));
      while (true) {
        int read = 0;
        Text key = new Text(), value = new Text();
        while (read < Record.RECORD_LENGTH) {
          long newRead = in.read(buffer, read, Record.RECORD_LENGTH - read);
          if (newRead == -1) {
            if (read == 0) {
              return records;
            } else {
              throw new EOFException("read past eof");
            }
          }
          read += newRead;
        }
        key.set(buffer, 0, Record.KEY_SIZE);
        value.set(buffer, Record.KEY_SIZE, Record.DATA_SIZE);
        records.add(new Record(key, value));
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read the file", e);
      throw new RuntimeException(e);
    }
  }

  public void save(Record[] records) {
    DataOutputStream os;
    try {
      os = new DataOutputStream(new FileOutputStream(outFileName));
      for (int i = 0; i < records.length; i++) {
        Record r = records[i];
        os.write(r.getKey().getBytes(), 0, Record.KEY_SIZE);
        os.write(r.getText().getBytes(), 0, Record.DATA_SIZE);
      }
      os.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new RuntimeException(e);
    }
  }

  public List<Record> getRecords() {
    return records;
  }
}
