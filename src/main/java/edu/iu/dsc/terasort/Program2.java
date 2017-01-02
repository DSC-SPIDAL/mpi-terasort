package edu.iu.dsc.terasort;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Status;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Program2 {
  private static Logger LOG = Logger.getLogger(Program2.class.getName());

  private String inputFolder;
  private String filePrefix;
  private String outputFolder;

  private int partitionSampleNodes;
  private int partitionSamplesPerNode;
  private int sendBufferSize;

  private int rank;
  private int worldSize;
  private Intracomm partitionCom;
  // local rank in the node
  private int localRank;

  public Program2(String []args) {
    readProgramArgs(args);
  }

  public static void main(String[] args) {
    try {
      MPI.Init(args);

      // execute the program
      Program2 program = new Program2(args);
      program.partialSendExecute();

      MPI.Finalize();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to tear down MPI");
    }
  }

  private void readProgramArgs(String []args) {
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
      sendBufferSize = Integer.parseInt(cmd.getOptionValue("sendBufferSize"));
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

    Thread t = new Thread(new ReceiveWorker(sorter, worldSize, sendBufferSize));
    t.start();

    // we are going to use pre-allocated worldSize buffers to send the correct values
    byte[][] sendBuffers = new byte[worldSize][];
    for (int i = 0; i < sendBuffers.length; i++) {
      sendBuffers[i] = new byte[sendBufferSize * Record.RECORD_LENGTH];
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

      byte []data = sendBuffers[partition];
      // now copy the content to the buffer
      System.arraycopy(records, i * Record.RECORD_LENGTH, data,
          sendBufferSizes[partition] * Record.RECORD_LENGTH, Record.RECORD_LENGTH);
      sendBufferSizes[partition] += 1;

      // now get the send buffer
      if (sendBufferSizes[partition] == sendBufferSize) {
        sendBuffer(sorter, sendBufferSizes[partition] * Record.RECORD_LENGTH, partition, data);
        sendBuffers[partition] = new byte[sendBufferSize * Record.RECORD_LENGTH];
        sendBufferSizes[partition] = 0;
      }
    }

    LOG.info("Last items to send rank: " + rank);

    // at the end send what is left
    for (int i = 0; i < worldSize; i++) {
      if (sendBufferSizes[i] != 0) {
        byte []data = sendBuffers[i];
        sendBuffer(sorter, sendBufferSizes[i] * Record.RECORD_LENGTH, i, data);
        sendBuffers[i] = new byte[sendBufferSize * Record.RECORD_LENGTH];
      }
    }

    LOG.info("Sending 0: " + rank);
    // send 0 to mark the end
    for (int i = 0; i < worldSize; i++) {
      if (i != rank) {
        int[] sizeBuffer = new int[1];
        sizeBuffer[0] = 0;
        MPI.COMM_WORLD.send(sizeBuffer, 1, MPI.INT, i, 100);
      }
    }

    try {
      t.join();
    } catch (InterruptedException e) {}

    // now sort the data
//    Record[] sortedRecords = sorter.sort();
//    loader.save(sortedRecords);
    MPI.COMM_WORLD.barrier();
  }

  private void sendBuffer(MergeSorter sorter, int size,
                          int sendRank, byte[] data) throws MPIException {
    int[] sizeBuffer = new int[1];
    if (sendRank != rank) {
      sizeBuffer[0] = size;
//      LOG.info("Sending size: " + sizeBuffer[0] + " to partition: " + partition + " from: " + rank);
      MPI.COMM_WORLD.send(sizeBuffer, 1, MPI.INT, sendRank, 100);
      // it is time to send this buffer
      LOG.info("Sending actual data of size: " + size + " sink: " + sendRank + " source: " + rank);
      MPI.COMM_WORLD.send(data, size, MPI.BYTE, sendRank, 100);
    } else {
      sorter.addData(rank, data);
    }
  }

  private class ReceiveWorker implements Runnable {
    private MergeSorter sorter;

    private int finishedSenders;

    public ReceiveWorker(MergeSorter sorter, int worldSize, int bufferSize) {
      this.sorter = sorter;
      this.finishedSenders = worldSize - 1;
    }

    @Override
    public void run() {
      while (true) {
        if (finishedSenders == 0) {
          break;
        }

        for (int source = 0; source < worldSize; source++) {
          if (source == rank) {
            continue;
          }

          try {
            Status status = MPI.COMM_WORLD.iProbe(source, 100);
            if (status == null) {
              continue;
            }
            int size = status.getCount(MPI.INT);
            if (size != 1) {
              throw new RuntimeException("Unexpected value, expecting 1 received: " + size);
            }

            int[] sizeBuffer = new int[1];
            MPI.COMM_WORLD.recv(sizeBuffer, 1, MPI.INT, source, 100);
            if (sizeBuffer[0] == 0) {
              LOG.info("Rank: " + source + " Finished sending");
              finishedSenders--;
            }

            byte []data = new byte[sizeBuffer[0]];
            MPI.COMM_WORLD.recv(data, sizeBuffer[0], MPI.BYTE, source, 100);
            LOG.info("Receved: " + sizeBuffer[0] + " rank: " + rank + " from: " + source);
            // sorter.addDataNonSorted(source, data);
          } catch (MPIException e) {
            LOG.log(Level.SEVERE, "MPI Exception", e);
            throw new RuntimeException(e);
          }
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
      LOG.info("rank: " + rank + " Partition tree: " + t.toString());
    }

    PartitionTree.TrieNode root = PartitionTree.buildTrie(partitions, 0, partitions.length, new Text(), 2);
    return new PartitionTree(root);
  }

}
