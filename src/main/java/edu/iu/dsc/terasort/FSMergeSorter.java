package edu.iu.dsc.terasort;

import edu.iu.dsc.terasort.heap.Heap;
import edu.iu.dsc.terasort.heap.HeapNode;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FSMergeSorter {
  private static Logger LOG = Logger.getLogger(FSMergeSorter.class.getName());

  private int maxRecordsInMemory;
  // number of records to read from file
  private int readSize = 100000;
  private String outFolder;
  private byte[] key = new byte[Record.KEY_SIZE];
  private byte[] text = new byte[Record.DATA_SIZE];
  private volatile List<Record[]> recordsList = new ArrayList<>();
  private long currentRecordsInMemory = 0;
  private int saveIndex = 0;
  private SortWorker sortWorker;
  private Thread sortWorkerThread;
  private String cacheFolder;
  private int rank;
  private Map<Integer, Integer> savedFileSizes = new HashMap<Integer, Integer>();
  private Map<Integer, OpenFile> fileParts = new HashMap<Integer, OpenFile>();

  private Lock lock = new ReentrantLock();
  private Condition notFull = lock.newCondition();

  private class OpenFile {
    FileChannel rwChannel;
    ByteBuffer os;

    public OpenFile(FileChannel rwChannel, ByteBuffer os) {
      this.rwChannel = rwChannel;
      this.os = os;
    }
  }

  public FSMergeSorter(int rank, int maxRecords, String outFolder, String cacheFolder) {
    this.maxRecordsInMemory = maxRecords;
    this.outFolder = outFolder;
    this.cacheFolder = cacheFolder;
    this.rank = rank;
    sortWorker = new SortWorker();
    sortWorkerThread = new Thread(sortWorker);
    sortWorkerThread.start();
  }

  public void add(ByteBuffer data, int size) {
    // for now lets get the keys and sort them
    int records = size / Record.RECORD_LENGTH;
    Record[] r = new Record[records];
    data.rewind();
    for (int i = 0; i < records; i++) {
      data.get(key, 0, Record.KEY_SIZE);
      data.get(text, 0, Record.DATA_SIZE);
      r[i] = new Record(new Text(key), new Text(text));
    }
    lock.lock();
    try {
      recordsList.add(r);
      currentRecordsInMemory += r.length;
      LOG.info(String.format("Rank %d add records %d", rank, currentRecordsInMemory));
      notFull.signal();
    } finally {
      lock.unlock();
    }
  }

  public void doneReceive() {
    sortWorker.stop();
    while (!sortWorker.hasStopped) {
      lock.lock();
      try {
        notFull.signal();
      } finally {
        lock.unlock();
      }
    }
    LOG.info(String.format("Rank %d Stopped receiving", rank));
  }

  public void merge() {
    // merge the remaiing lists
    Record[][] records = new Record[recordsList.size()][];
    for (int i = 0; i < recordsList.size(); i++) {
      records[i] = recordsList.get(i);
      Arrays.sort(records[i]);
    }
    long totalToSave = 0;
    Record[] inMemoryRecords = merge(records, records.length);
    totalToSave += inMemoryRecords.length;

    for (int i = 0; i < saveIndex; i++) {
      totalToSave += savedFileSizes.get(i);
    }

    int saveFileIndex = 0;
    int k = saveIndex + 1;
    // create index pointer for every list.
    int[] ptrs = new int[k];
    for (int i = 0; i < ptrs.length; i++) {
      ptrs[i] = 0;
    }

    int resultIndex = 0;
    Record[] result = new Record[maxRecordsInMemory];

    // how many records we have read from the file so far
    Map<Integer, Integer> currentReadSizes = new HashMap<>();
    for (int i = 0; i < saveIndex; i++) {
      currentReadSizes.put(i, 0);
    }

    try {
      // open the saved files
      openSavedFiles();
      // read initial data from the open file

      // create a heap with number of saved files + 1
      Heap heap = new Heap(k);

      Record[][] A = new Record[k][];
      A[0] = inMemoryRecords;
      for (int i = 1; i < k; i++) {
        int currentRead = currentReadSizes.get(i - 1);
        int fileSize = savedFileSizes.get(i - 1);
        int sizeToRead = currentRead + readSize > fileSize ? fileSize - currentRead : readSize;
//        LOG.info(String.format("Rank %d From %d Size to read %d currentRead %d fileSize %d", rank, i - 1, sizeToRead, currentRead, fileSize));
        A[i] = read(fileParts.get(i - 1), sizeToRead);
        currentRead += sizeToRead;
        currentReadSizes.put(i - 1, currentRead);
      }

      // initial insertion
      for (int i = 0; i < k; i++) {
        if (ptrs[i] < A[i].length) {
          heap.insert(A[i][ptrs[i]], i);
        } else {
          heap.insert(new Record(new Text(Heap.largest)), i);
        }
      }

      long count = 0;
      while (count < totalToSave) {
        HeapNode h = heap.extractMin();
        result[resultIndex] = h.data;
        resultIndex++;

        if (resultIndex == result.length) {
          String outFileName = Paths.get(outFolder, rank + "_" + saveFileIndex).toString();
//          LOG.info(String.format("Rank %d saving size %d index %d", rank, resultIndex, saveFileIndex));
          DataLoader.saveFast(result, resultIndex, outFileName);
          saveFileIndex++;
          resultIndex = 0;
        }

        ptrs[h.listNo]++;
        if (ptrs[h.listNo] < A[h.listNo].length) {
          heap.insert(A[h.listNo][ptrs[h.listNo]], h.listNo);
        } else {
          if (h.listNo != 0) {
            // now check weather we have data in file
            int i = h.listNo;
            int currentRead = currentReadSizes.get(i - 1);
            int fileSize = savedFileSizes.get(i - 1);
            int sizeToRead = currentRead + readSize > fileSize ? fileSize - currentRead : readSize;
//            LOG.info(String.format("Rank %d SizeToRead %d currentRead %d fileSize %d readSize %d count %d totalToSave %d ptrs[h.listNo] %d A[h.listNo].length %d", rank, sizeToRead, currentRead, fileSize, readSize, count, totalToSave, ptrs[h.listNo], A[h.listNo].length));
            if (sizeToRead > 0) {
              A[h.listNo] = read(fileParts.get(i - 1), sizeToRead);
              ptrs[h.listNo] = 0;
              heap.insert(A[h.listNo][ptrs[h.listNo]], h.listNo);
              // ptrs[h.listNo]++;
              currentRead += sizeToRead;
              currentReadSizes.put(i - 1, currentRead);
            } else {
//              LOG.info(String.format("Rank %d Inserting largest %d", rank, h.listNo));
              heap.insert(new Record(new Text(Heap.largest)), h.listNo);
            }
          } else {
            heap.insert(new Record(new Text(Heap.largest)), h.listNo);
          }
        }
        count++;
      }
//      LOG.info(String.format("Rank %d total save %d", rank, totalToSave));
      if (resultIndex > 0) {
        String outFileName = Paths.get(outFolder, rank + "_" + saveFileIndex).toString();
//        LOG.info(String.format("Rank %d *** saving size %d index %d", rank, resultIndex, saveFileIndex));
        DataLoader.saveFast(result, resultIndex, outFileName);
      }
    } finally {
      for (OpenFile f : fileParts.values()) {
        try {
          f.rwChannel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void openSavedFiles() {
    for (int i = 0; i < saveIndex; i++) {
      fileParts.put(i, openSavedPart(i, savedFileSizes.get(i) * Record.RECORD_LENGTH));
    }
  }

  public static Record[] merge(Record[][] A, int k) {
    Heap heap = new Heap(k);

    int nk = 0;
    for (int i = 0; i < A.length; i++) {
      nk += A[i].length;
    }
    Record[] result = new Record[nk];
    int count = 0;
    int[] ptrs = new int[k];
    // create index pointer for every list.
    for (int i = 0; i < ptrs.length; i++) {
      ptrs[i] = 0;
    }
    for (int i = 0; i < k; i++) {
      if (ptrs[i] < A[i].length) {
        heap.insert(A[i][ptrs[i]], i);
      } else {
        heap.insert(new Record(new Text(Heap.largest)), i);
      }
    }
    while (count < nk) {
      HeapNode h = heap.extractMin();
      result[count] = h.data;
      ptrs[h.listNo]++;
      if (ptrs[h.listNo] < A[h.listNo].length) {
        heap.insert(A[h.listNo][ptrs[h.listNo]], h.listNo);
      } else {
//        LOG.info("******************************************************** ");
        heap.insert(new Record(new Text(Heap.largest)), h.listNo);
      }
      count++;
    }
    return result;
  }

  /**
   * A worker class to
   */
  private class SortWorker implements Runnable {
    private volatile boolean run = true;
    private volatile boolean hasStopped = false;
    @Override
    public void run() {
      while (run) {
        // check weather we have enough records
//        LOG.info(String.format("rank %d max records %d current records %d", rank, maxRecordsInMemory, currentRecordsInMemory));
        if  (currentRecordsInMemory >= maxRecordsInMemory) {
          // now save to disk
          List<Record[]> list;
          lock.lock();
          try {
            list = recordsList;
            recordsList = new ArrayList<>();
            currentRecordsInMemory = 0;
          } finally {
            lock.unlock();
          }

          Record[][] records = new Record[list.size()][];
          for (int i = 0; i < records.length; i++) {
            records[i] = list.get(i);
            Arrays.sort(records[i]);
          }

          Record[] save = merge(records, records.length);
          String outFileName = Paths.get(cacheFolder, rank + "_" + saveIndex).toString();
//          LOG.info(String.format("Rank %d Saving to file: %s with size %d", rank, outFileName, save.length));
          saveFile(save, save.length, outFileName);
          savedFileSizes.put(saveIndex, save.length);
          saveIndex++;
        } else {
          lock.lock();
          try {
            if (run) {
              notFull.await();
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          } finally {
            lock.unlock();
          }
        }
      }
      hasStopped = true;
//      LOG.info(String.format("Rank %d thread stopped **** ", rank));
    }

    public void stop() {
      lock.lock();
      try {
        run = false;
      }finally {
        lock.unlock();
      }
    }

    public boolean isHasStopped() {
      return hasStopped;
    }
  }

  private Record[] read(OpenFile file, int size) {
    ByteBuffer b = file.os;
    Record[] r = new Record[size];
    for (int i = 0; i < size; i++) {
      b.get(key);
      b.get(text);
      r[i] = new Record(new Text(key), new Text(text));
    }
    return r;
  }

  private OpenFile openSavedPart(int part, long length) {
    String outFileName = Paths.get(cacheFolder, rank + "_" + part).toString();
    FileChannel rwChannel = null;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, 0, length);
      return new OpenFile(rwChannel, os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void saveFile(Record[] records, int size, String outFileName) {
    try {
      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, records.length * Record.RECORD_LENGTH);
      for (int i = 0; i < size; i++) {
        Record r = records[i];
        os.put(r.getKey().getBytes(), 0, Record.KEY_SIZE);
        os.put(r.getText().getBytes(), 0, Record.DATA_SIZE);
      }
      rwChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new RuntimeException(e);
    }
  }
}
