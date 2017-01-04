package edu.iu.dsc.terasort;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Program2 {
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
    MergeSorter sorter = new MergeSorter(rank);

    // create the partitioned record list
    Map<Integer, List<Integer>> partitionedRecords = new HashMap<Integer, List<Integer>>();
    for (int i = 0; i < worldSize; i++) {
      partitionedRecords.put(i, new ArrayList<>());
    }

    String inputFile = Paths.get(inputFolder, filePrefix + Integer.toString(localRank)).toString();
    String outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(rank)).toString();
    DataLoader loader = new DataLoader(inputFile, outputFile);
    byte []records = loader.loadArray(rank);
    int numberOfRecords = records.length / Record.RECORD_LENGTH;
    LOG.info("Rank: " + rank + " Loaded records: " + records.length);
    PartitionTree partitionTree = buildPartitionTree(records);
    MPI.COMM_WORLD.barrier();

    // number of records in each buffer
    byte[] keyBuffer = new byte[Record.KEY_SIZE];
    // now go through the partitions and add them to correct sendbuffer
    for (int i = 0; i < numberOfRecords; i++) {
      System.arraycopy(records, i * Record.RECORD_LENGTH, keyBuffer, 0, Record.KEY_SIZE);

      Record r = new Record(new Text(keyBuffer));
      int partition = partitionTree.getPartition(r.getKey());
      partitionedRecords.get(partition).add(i);
    }

    ByteBuffer recvBuffer = MPI.newByteBuffer(maxSendRecordsBytes * worldSize);
    ByteBuffer sendBuffer = MPI.newByteBuffer(maxSendRecordsBytes);
    // go through each partition
    for (int i = 0; i < worldSize; i++) {
      // now lets go through each partition and do a gather
      // first lest send the expected amounts to each process
      IntBuffer expectedAmountSendBuffer = MPI.newIntBuffer(1);
      // we pre-allocate this buffer as this is the max amount we are going to send at each time
      IntBuffer expectedAmountReceiveBuffer = MPI.newIntBuffer(worldSize);
      expectedAmountSendBuffer.put(partitionedRecords.get(i).size() * Record.RECORD_LENGTH);
      long allGatherStart = System.nanoTime();
      MPI.COMM_WORLD.gather(expectedAmountSendBuffer, 1, MPI.INT, expectedAmountReceiveBuffer, 1, MPI.INT, i);
      double elapsedMillis = ((double)System.nanoTime() - allGatherStart) / 1000000.0;
      LOG.info(String.format("Rank: %d allgather time: %f", rank, elapsedMillis));

      int maxRounds = 0;
      if (rank == i) {
        for (int j = 0; j < worldSize; j++) {
          int temp = new Double(Math.ceil((double) expectedAmountReceiveBuffer.get(j) / maxSendRecordsBytes)).intValue();
          if (temp > maxRounds) {
            maxRounds = temp;
          }
        }
      }

      IntBuffer maxRoundsBuffer = MPI.newIntBuffer(1);
      maxRoundsBuffer.put(maxRounds);
      MPI.COMM_WORLD.bcast(maxRoundsBuffer, 1, MPI.INT, i);
      maxRoundsBuffer.rewind();
      maxRounds = maxRoundsBuffer.get();

      int round = 0;
      // now do several gathers to gather all the data from the buffers
      while (round < maxRounds) {
        int totalSize = 0;
        int[] receiveSizes = new int[worldSize];
        int[] displacements = new int[worldSize];
        displacements[0] = 0;
        if (rank == i) {
          for (int j = 0; j < worldSize; j++) {
            int temp = new Double(Math.ceil((double) expectedAmountReceiveBuffer.get(j) / (maxRounds * Record.RECORD_LENGTH))).intValue();
            if (temp * (round + 1) > expectedAmountReceiveBuffer.get(j) / Record.RECORD_LENGTH) {
              temp = expectedAmountReceiveBuffer.get(j) / Record.RECORD_LENGTH - temp * (round);
            }
            receiveSizes[j] = temp * Record.RECORD_LENGTH;
            totalSize += receiveSizes[j];
            if (j > 0) {
              displacements[j] = displacements[j - 1] + receiveSizes[j - 1];
            }
          }
        }

        // copy the data
        sendBuffer.rewind();
        List<Integer> partitionedKeys = partitionedRecords.get(i);
        int sendSize = new Double(Math.ceil((double)partitionedKeys.size() / (maxRounds))).intValue();
        if (sendSize * (round + 1) > partitionedKeys.size()) {
          sendSize = partitionedKeys.size() - sendSize * (round);
        }

        try {
          for (int j = 0; j < sendSize; j++) {
            int k = round * sendSize + j;
            int recordPosition = partitionedKeys.get(k);
            sendBuffer.put(records, recordPosition * Record.RECORD_LENGTH, Record.RECORD_LENGTH);
          }
        } catch (IndexOutOfBoundsException e) {
          throw new RuntimeException(e);
        }

        try {
          allGatherStart = System.nanoTime();
          recvBuffer.rewind();
          LOG.info(String.format("Rank: %d start gathering", rank));
          MPI.COMM_WORLD.gatherv(sendBuffer, sendSize, MPI.BYTE, recvBuffer, receiveSizes, displacements, MPI.BYTE, i);
          if (i == rank) {
            elapsedMillis = ((double)System.nanoTime() - allGatherStart) / 1000000.0;
            LOG.info(String.format("Rank: %d gather time: %f", rank, elapsedMillis));
            sorter.addData(recvBuffer, totalSize);
          }
        } catch (ArrayIndexOutOfBoundsException e) {
          LOG.log(Level.INFO, "Rank: " + rank, e);
          throw new RuntimeException(e);
        }
        round++;
      }
    }
    Record[] sortedRecords = sorter.sort();
    loader.save(sortedRecords);
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
      // LOG.info("rank: " + rank + " Partition tree: " + t.toString());
    }

    PartitionTree.TrieNode root = PartitionTree.buildTrie(partitions, 0, partitions.length, new Text(), 2);
    return new PartitionTree(root);
  }

}
