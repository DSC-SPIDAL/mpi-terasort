package edu.iu.dsc.terasort;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

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
      int rank = MPI.COMM_WORLD.getRank();
      int worldSize = MPI.COMM_WORLD.getSize();
      int[] send = new int[rank];
      for (int i = 0; i < rank; i++) {
        send[i] = i;
      }
      int[] recv = new int[10];
      int[] sizes = new int[]{0, 1, 2, 3};
      int[] disla = new int[]{0, 0, 1, 3};
      // MPI.COMM_WORLD.gatherv(send, rank, MPI.INT, recv, sizes, disla, MPI.INT, 0);
      String s = "";
      if (rank == 0) {
        for (int i : recv) {
          s += i;
        }
        System.out.println("s: " + s);
      }
      System.out.println("Done gathering");
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
    MergeSorter sorter = new MergeSorter(worldSize);

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
      partitionedRecords.get(partition).add(partition);
    }


    // go through each partition
    for (int i = 0; i < worldSize; i++) {
      // now lets go through each partition and do a gather
      // first lest send the expected amounts to each process
      int[] expectedAmountSendBuffer = new int[1];
      // we pre-allocate this buffer as this is the max amount we are going to send at each time
      int[] expectedAmountReceiveBuffer = new int[worldSize];
      expectedAmountSendBuffer[0] = partitionedRecords.get(i).size() * Record.RECORD_LENGTH;
      MPI.COMM_WORLD.allGather(expectedAmountSendBuffer, 1, MPI.INT, expectedAmountReceiveBuffer, 1, MPI.INT);
      if (i == rank) {
        String s = "";
        for (int j = 0; j < worldSize; j++) {
          s = s + expectedAmountReceiveBuffer[j] + " ";
        }
        LOG.info("Rank: " + rank + "Expecting: " + s);
      }

      int round = 0;

      // now do several gathers to gather all the data from the buffers
      while (true) {
        int totalSize = 0;
        String s = "";
        String s2 = "";
        int[] receiveSizes = new int[worldSize];
        int[] displacements = new int[worldSize];
        displacements[0] = 0;
        for (int j = 0; j < worldSize; j++) {
          receiveSizes[j] = expectedAmountReceiveBuffer[j] > round * maxSendRecordsBytes + maxSendRecordsBytes ? maxSendRecordsBytes :
              expectedAmountReceiveBuffer[j] - round * maxSendRecordsBytes;
          if (receiveSizes[j] < 0) receiveSizes[j] = 0;
          s = s + receiveSizes[j] + " ";
          totalSize += receiveSizes[j];
          if (j > 0) {
            displacements[j] = displacements[j - 1] + receiveSizes[j - 1];
          }
          s2 = s2 + displacements[j] + " ";
        }
        // if (receiveSizes[0] != maxSendRecordsBytes) break;
        LOG.info("Rank: " + rank + "Receive sizes: " + s);
        LOG.info("Rank: " + rank + "Displacement sizes: " + s2);

        if (totalSize > 0) {
          // copy the data
          byte[] sendBuffer = new byte[receiveSizes[rank]];
          List<Integer> partitionedKeys = partitionedRecords.get(i);
          for (int j = 0; j < receiveSizes[rank] / Record.RECORD_LENGTH; j++) {
            int k = round * maxSendRecords + j;
            int recordPosition = partitionedKeys.get(k);
            System.arraycopy(records, recordPosition * Record.RECORD_LENGTH, sendBuffer, j * Record.RECORD_LENGTH, Record.RECORD_LENGTH);
          }

          byte []recv = new byte[totalSize];
          LOG.info("Gather source: " + i + " total: " + totalSize + " this proc: " + rank + " send size: " + receiveSizes[rank]);
          try {
             MPI.COMM_WORLD.gatherv(sendBuffer, receiveSizes[rank], MPI.BYTE, recv, receiveSizes, displacements, MPI.BYTE, i);
          } catch (ArrayIndexOutOfBoundsException e) {
            LOG.log(Level.INFO, "Rank: " + rank, e);
            throw new RuntimeException(e);
          }
        } else {
          LOG.info("Rank: " + rank + " done");
          break;
        }
        round++;
      }
      LOG.info("Rank: " + rank + " done done");
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
