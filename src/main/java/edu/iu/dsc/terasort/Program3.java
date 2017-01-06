package edu.iu.dsc.terasort;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Status;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Program3 {
  private static Logger LOG = Logger.getLogger(Program2.class.getName());

  private String inputFolder;
  private String filePrefix;
  private String outputFolder;

  private int partitionSampleNodes;
  private int partitionSamplesPerNode;
  private int maxSendRecords;
  private int maxSendRecordsBytes;

  private int rank;
  private int worldSize;
  private Intracomm partitionCom;
  // local rank in the node
  private int localRank;

  private Lock lock;

  public Program3(String []args) {
    readProgramArgs(args);
  }

  public static void main(String[] args) {
    try {
      MPI.InitThread(args, MPI.THREAD_MULTIPLE);
      // execute the program
      Program3 program = new Program3(args);
      program.partialSendExecute();


      MPI.Finalize();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to tear down MPI");
    }
  }

  private void readProgramArgs(String []args) {
    lock = new ReentrantLock();
    Options options = new Options();
    options.addOption("input", true, "Input directory");
    options.addOption("output", true, "Output directory");
    options.addOption("partitionSampleNodes", true, "Number of nodes to choose partition samples");
    options.addOption("partitionSamplesPerNode", true, "Number of samples to choose from each node");
    options.addOption("filePrefix", true, "Prefix of the file partition");
    options.addOption("sendBufferSize", true, "Send buffer size for shuffling");

    CommandLineParser commandLineParser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = commandLineParser.parse(options, args);
      inputFolder = cmd.getOptionValue("input");
      outputFolder = cmd.getOptionValue("output");
      partitionSampleNodes = Integer.parseInt(cmd.getOptionValue("partitionSampleNodes"));
      partitionSamplesPerNode = Integer.parseInt(cmd.getOptionValue("partitionSamplesPerNode"));
      filePrefix = cmd.getOptionValue("filePrefix");
      maxSendRecords = Integer.parseInt(cmd.getOptionValue("sendBufferSize"));
      maxSendRecordsBytes = maxSendRecords * Record.RECORD_LENGTH;
    } catch (ParseException e) {
      LOG.log(Level.SEVERE, "Failed to read the options", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("program", options);
      throw new RuntimeException(e);
    }
  }

  public void partialSendExecute() throws MPIException {
    rank = MPI.COMM_WORLD.getRank();
    worldSize = MPI.COMM_WORLD.getSize();
    localRank = Integer.parseInt(System.getenv("OMPI_COMM_WORLD_LOCAL_RANK"));
    LOG.info("Local rank: " + localRank);
    MergeSorter sorter = new MergeSorter(worldSize);

    String inputFile = Paths.get(inputFolder, filePrefix + Integer.toString(localRank)).toString();
    String outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(rank)).toString();
    DataLoader loader = new DataLoader(inputFile, outputFile);
    byte []records = loader.loadArray(rank);
    int numberOfRecords = records.length / Record.RECORD_LENGTH;
    LOG.info("Rank: " + rank + " Loaded records: " + records.length);
    PartitionTree partitionTree = buildPartitionTree(records);
    MPI.COMM_WORLD.barrier();

    Thread t = new Thread(new ReceiveWorker(sorter, worldSize, maxSendRecords));
    t.start();

    // we are going to use pre-allocated worldSize buffers to send the correct values
    ByteBuffer[] sendBuffers = new ByteBuffer[worldSize];
    for (int i = 0; i < sendBuffers.length; i++) {
      sendBuffers[i] =  MPI.newByteBuffer(maxSendRecords * Record.RECORD_LENGTH);
    }

    // number of records in each buffer
    int[] sendBufferSizes = new int[worldSize];
    Arrays.fill(sendBufferSizes, 0);
    byte[] keyBuffer = new byte[Record.KEY_SIZE];

    // now go through the partitions and add them to correct sendbuffer
    for (int i = 0; i < numberOfRecords; i++) {
      System.arraycopy(records, i * Record.RECORD_LENGTH, keyBuffer, 0, Record.KEY_SIZE);

      Record r = new Record(new Text(keyBuffer));
      int partition = partitionTree.getPartition(r.getKey());

      ByteBuffer data = sendBuffers[partition];
      // now copy the content to the buffer
      data.put(records, i * Record.RECORD_LENGTH, Record.RECORD_LENGTH);
      sendBufferSizes[partition] += 1;

      // now get the send buffer
      if (sendBufferSizes[partition] == maxSendRecords) {
        sendBuffer(sorter, sendBufferSizes[partition] * Record.RECORD_LENGTH, partition, data);
        sendBufferSizes[partition] = 0;
        data.rewind();
      }
    }

    LOG.info("Last items to send rank: " + rank);

    // at the end send what is left
    for (int i = 0; i < worldSize; i++) {
      if (sendBufferSizes[i] != 0) {
        ByteBuffer data = sendBuffers[i];
        sendBuffer(sorter, sendBufferSizes[i] * Record.RECORD_LENGTH, i, data);
      }
    }

    LOG.info("Sending 0: " + rank);
    // send 0 to mark the end
    ByteBuffer sizeBuffer = MPI.newByteBuffer(1);
    for (int i = 0; i < worldSize; i++) {
      if (i != rank) {
        sizeBuffer.rewind();
        sizeBuffer.put(new Integer(0).byteValue());
        MPI.COMM_WORLD.send(sizeBuffer, 1, MPI.BYTE, i, 100);
      }
    }

    try {
      t.join();
    } catch (InterruptedException e) {}

    // now sort the data
    Record[] sortedRecords = sorter.sort();
    loader.save(sortedRecords);
    MPI.COMM_WORLD.barrier();
  }

  private void sendBuffer(MergeSorter sorter, int size,
                          int sendRank, ByteBuffer data) throws MPIException {
    IntBuffer sizeBuffer = MPI.newIntBuffer(1);
    if (sendRank != rank) {
      sizeBuffer.put(size);
      LOG.info(String.format("Rank: %d sending size: %d to: %d", rank, size, sendRank));
      MPI.COMM_WORLD.sSend(data, size, MPI.BYTE, sendRank, 100);
    } else {
      sorter.addData(data, size);
    }
  }

  private class ReceiveWorker implements Runnable {
    private MergeSorter sorter;

    private int finishedSenders;
    ByteBuffer data;

    public ReceiveWorker(MergeSorter sorter, int worldSize, int bufferSize) {
      this.sorter = sorter;
      this.finishedSenders = worldSize - 1;
      data = MPI.newByteBuffer(bufferSize * Record.RECORD_LENGTH);
    }

    @Override
    public void run() {
      while (true) {
        if (finishedSenders == 0) {
          break;
        }

        try {
          Status status = MPI.COMM_WORLD.iProbe(MPI.ANY_SOURCE, 100);
          if (status == null) {
            continue;
          }
          int size = status.getCount(MPI.BYTE);

          if (size == 1) {
            MPI.COMM_WORLD.recv(data, size, MPI.BYTE, status.getSource(), 100);
            finishedSenders--;
            continue;
          }

          LOG.info(String.format("Rank: %d recv 2", rank));
          MPI.COMM_WORLD.recv(data, size, MPI.BYTE, status.getSource(), 100);
          LOG.info(String.format("Rank: %d received %d from %d", rank, size, status.getSource()));
          sorter.addData(data, size);
        } catch (MPIException e) {
          LOG.log(Level.SEVERE, "MPI Exception", e);
          throw new RuntimeException(e);
        }
      }
    }
  }


  private PartitionTree buildPartitionTree(byte[] records) throws MPIException {
    // first create the partition communicator
    if (rank < partitionSampleNodes) {
      partitionCom = MPI.COMM_WORLD.split(0, rank);
    } else {
      partitionCom = MPI.COMM_WORLD.split(1, rank);
    }
    byte[] keyBuffer = new byte[Record.KEY_SIZE];
    Record[] partitionRecords = new Record[partitionSamplesPerNode];
    for (int i = 0; i < partitionSamplesPerNode; i++) {
      Text key = new Text();
      System.arraycopy(records, i * Record.RECORD_LENGTH, keyBuffer, 0, Record.KEY_SIZE);
      key.set(keyBuffer, 0, Record.KEY_SIZE);
      partitionRecords[i] = new Record(key);
    }

    DataPartitioner partitioner = new DataPartitioner(rank, worldSize,
        partitionSampleNodes, partitionSamplesPerNode, partitionCom);
    byte[] selectedKeys = partitioner.execute(partitionRecords);

    int noOfPartitions = worldSize - 1;
    if (selectedKeys.length / Record.KEY_SIZE != noOfPartitions) {
      String msg = "Selected keys( " + selectedKeys.length / Record.KEY_SIZE
          + " ) generated is not equal to: " + noOfPartitions;
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }
    // now build the tree
    Text[] partitions = new Text[noOfPartitions];
    for (int i = 0; i < noOfPartitions; i++) {
      Text t = new Text();
      t.set(selectedKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);

      partitions[i] = t;
    }

    PartitionTree.TrieNode root = PartitionTree.buildTrie(partitions, 0, partitions.length, new Text(), 2);
    return new PartitionTree(root);
  }
}
