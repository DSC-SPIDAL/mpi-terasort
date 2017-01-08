package edu.iu.dsc.terasort;

import mpi.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Program5 {
  private static Logger LOG = Logger.getLogger(Program4.class.getName());

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

  public Program5(String []args) {
    readProgramArgs(args);
  }

  public static void main(String[] args) {
    try {
      MPI.Init(args);
      // execute the program
      Program5 program = new Program5(args);
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
    long startTime = System.currentTimeMillis();
    rank = MPI.COMM_WORLD.getRank();
    worldSize = MPI.COMM_WORLD.getSize();
    localRank = Integer.parseInt(System.getenv("OMPI_COMM_WORLD_LOCAL_RANK"));
    LOG.info("Local rank: " + localRank);
    MergeSorter sorter = new MergeSorter(rank);
    // create the partitioned record list
    Map<Integer, List<Integer>> partitionedRecords = new HashMap<Integer, List<Integer>>();
    for (int i = 0; i < worldSize; i++) {
      partitionedRecords.put(i, new ArrayList<>());
    }

    long readStartTime = System.currentTimeMillis();
    String inputFile = Paths.get(inputFolder, filePrefix + Integer.toString(localRank)).toString();
    String outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(rank)).toString();
    DataLoader loader = new DataLoader(inputFile, outputFile);
    byte []records = loader.loadArray(rank);
    MPI.COMM_WORLD.barrier();
    long readEndTime = System.currentTimeMillis();
    int numberOfRecords = records.length / Record.RECORD_LENGTH;
    LOG.info("Rank: " + rank + " Loaded records: " + records.length);

    long partitionStartTime = System.currentTimeMillis();
    PartitionTree partitionTree = buildPartitionTree(records);

    // number of records in each buffer
    byte[] keyBuffer = new byte[Record.KEY_SIZE];
    // now go through the partitions and add them to correct sendbuffer
    for (int i = 0; i < numberOfRecords; i++) {
      System.arraycopy(records, i * Record.RECORD_LENGTH, keyBuffer, 0, Record.KEY_SIZE);

      Record r = new Record(new Text(keyBuffer));
      int partition = partitionTree.getPartition(r.getKey());
      partitionedRecords.get(partition).add(i);
    }
    MPI.COMM_WORLD.barrier();
    long partitionEndTime = System.currentTimeMillis();

    // lets assume this is enough
    ByteBuffer sendBuffer = MPI.newByteBuffer(records.length * 4 / worldSize);
    ByteBuffer receiveBuffer = MPI.newByteBuffer(records.length * 4 / worldSize);
    // go through each partition
    long datShuffleStartTime = System.currentTimeMillis();
    for (int i = 1; i < worldSize; i++) {
      // now lets go through each partition and do a gather
      // first lest send the expected amounts to each process
      // we pre-allocate this buffer as this is the max amount we are going to send at each time
      int sendingRank = (rank + i) % worldSize;
      int receivingRank = (rank - i + worldSize) % worldSize;

      // now do several gathers to gather all the data from the buffers
      List<Integer> sendingPartitionedKeys = partitionedRecords.get(sendingRank);
      receiveBuffer.rewind();
      sendBuffer.rewind();

      for (int j = 0; j < sendingPartitionedKeys.size(); j++) {
        int recordPosition = sendingPartitionedKeys.get(j);
        sendBuffer.put(records, recordPosition * Record.RECORD_LENGTH, Record.RECORD_LENGTH);
      }
      Request sendRequest = MPI.COMM_WORLD.iSend(sendBuffer, sendingPartitionedKeys.size() * Record.RECORD_LENGTH, MPI.BYTE, sendingRank, 100);

      boolean sendCoomplete = false;
      boolean receiveComplete = false;
      while (!sendCoomplete || !receiveComplete) {
        Status receiceStatus = MPI.COMM_WORLD.iProbe(receivingRank, 100);
        if (receiceStatus != null && !receiveComplete) {
          int count = receiceStatus.getCount(MPI.BYTE);
          MPI.COMM_WORLD.recv(receiveBuffer, count, MPI.BYTE, receivingRank, 100);
          receiveComplete = true;
          sorter.addData(receiveBuffer, count);
          receiveBuffer.rewind();
        }
        Status sendStatus = sendRequest.testStatus();
        if (sendStatus != null && !sendCoomplete) {
          sendCoomplete = true;
        }
      }

      if (i % (worldSize / 2) == 0) {
        LOG.info(String.format("Rank %d progressing by sending to %d and receiving %d", rank, sendingRank, receivingRank));
      }
    }

    LOG.info(String.format("Rank %d done bulk sending", rank));
    List<Integer> ownPartitions = partitionedRecords.get(rank);
    receiveBuffer.rewind();

    for (int j = 0; j < ownPartitions.size(); j++) {
      int recordPosition = ownPartitions.get(j);
      receiveBuffer.put(records, recordPosition * Record.RECORD_LENGTH, Record.RECORD_LENGTH);
    }
    receiveBuffer.rewind();
    sorter.addData(receiveBuffer, ownPartitions.size() * Record.RECORD_LENGTH);
    MPI.COMM_WORLD.barrier();
    long dataShuffleEndTime = System.currentTimeMillis();

    long sortingTime = System.currentTimeMillis();
    Record[] sortedRecords = sorter.sort();
    MPI.COMM_WORLD.barrier();
    long sortingEndTime = System.currentTimeMillis();

    long saveTime = System.currentTimeMillis();
    loader.saveFast(sortedRecords);
    MPI.COMM_WORLD.barrier();
    long saveEndTime = System.currentTimeMillis();

    MPI.COMM_WORLD.barrier();
    if (rank == 0) {
      LOG.info("Total time: " + (System.currentTimeMillis() - startTime));
      LOG.info("Read time: " + (readEndTime - readStartTime));
      LOG.info("Partition time: " + (partitionEndTime - partitionStartTime));
      LOG.info("Shuffle time: " + (dataShuffleEndTime - datShuffleStartTime));
      LOG.info("Sort time: " + (sortingEndTime - sortingTime));
      LOG.info("Save time: " + (saveEndTime - saveTime));
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
