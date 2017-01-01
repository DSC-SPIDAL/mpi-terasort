package edu.iu.dsc.terasort;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Program {
  private static Logger LOG = Logger.getLogger(Program.class.getName());

  private String inputFolder;
  private String filePrefix;
  private String outputFolder;

  private int partitionSampleNodes;
  private int partitionSamplesPerNode;

  private int rank;
  private int worldSize;
  private Intracomm partitionCom;
  // local rank in the node
  private int localRank;

  public Program(String []args) {
    readProgramArgs(args);
  }

  public static void main(String[] args) {
    try {
      MPI.Init(args);

      // execute the program
      Program program = new Program(args);
      int rank = MPI.COMM_WORLD.getRank();
      int size = MPI.COMM_WORLD.getSize();
      int[] send = new int[]{1,2,3,4};
      int[] recv = new int[4 * size];
      MPI.COMM_WORLD.gather(send, 4, MPI.INT, recv, 4, MPI.INT, 0);
      if (rank == 0) {
        for (int i : recv) {
          System.out.println(i);
        }
      }
      program.execute();

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

    CommandLineParser commandLineParser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = commandLineParser.parse(options, args);
      inputFolder = cmd.getOptionValue("input");
      outputFolder = cmd.getOptionValue("output");
      partitionSampleNodes = Integer.parseInt(cmd.getOptionValue("partitionSampleNodes"));
      partitionSamplesPerNode = Integer.parseInt(cmd.getOptionValue("partitionSamplesPerNode"));
      filePrefix = cmd.getOptionValue("filePrefix");
    } catch (ParseException e) {
      LOG.log(Level.SEVERE, "Failed to read the options", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("program", options);
      throw new RuntimeException(e);
    }
  }

  public void execute() throws MPIException {
    rank = MPI.COMM_WORLD.getRank();
    worldSize = MPI.COMM_WORLD.getSize();
    localRank = Integer.parseInt(System.getenv("OMPI_COMM_WORLD_LOCAL_RANK"));
    LOG.info("Local rank: " + localRank);
    MergeSorter sorter = new MergeSorter(worldSize);

    String inputFile = Paths.get(inputFolder, filePrefix + Integer.toString(localRank)).toString();
    String outputFile = Paths.get(outputFolder, filePrefix + Integer.toString(rank)).toString();
    DataLoader loader = new DataLoader(inputFile, outputFile);
    List<Record> records = loader.load(rank);
    LOG.info("Rank: " + rank + " Loaded records: " + records.size());
    PartitionTree partitionTree = buildPartitionTree(records);

    // now sort the records
    Collections.sort(records);

    // allocate a buffer big enough to hold all the records
    byte[] allRecords = new byte[records.size() * Record.RECORD_LENGTH];

    // now go through the partitions and send them to correct nodes
    int currentPartition = 0;
    int currentPartitionStart = 0;
    for (int i = 0; i < records.size(); i++) {
      Record r = records.get(i);
      System.arraycopy(r.getKey().getBytes(), 0, allRecords, i * Record.RECORD_LENGTH, Record.KEY_SIZE);
      System.arraycopy(r.getText().getBytes(), 0, allRecords, i * Record.RECORD_LENGTH + Record.KEY_SIZE, Record.DATA_SIZE);
      int partition = partitionTree.getPartition(r.getKey());
      // LOG.log(Level.INFO, "Partition is: " + partition);

      // we are changing partitions or this is the last
      if (partition != currentPartition || (i == records.size() - 1) && i != 0) {
        if (i == records.size() - 1) {
          i++;
        }

        LOG.log(Level.INFO, "Rank: " + rank + " changing partition from: " + currentPartition + " to: " + partition);
        // now lets look at the rank
        int[] size = new int[1];
        if (rank == currentPartition) {
          // do receive
          for (int j = 0; j < worldSize; j++) {
            if (j != rank) {
              LOG.log(Level.INFO, "Doing receive on rank: " + rank + " expecting from: " + j);
              // first receive size
              MPI.COMM_WORLD.recv(size, 1, MPI.INT, j, 99);
              // now receive data
              byte[] data = new byte[size[0]];
              MPI.COMM_WORLD.recv(data, size[0], MPI.BYTE, j, 99);
              sorter.addData(j, data);
            } else {
              // nothing to receive, local copy
              int bytes = (i - currentPartitionStart) * Record.RECORD_LENGTH;
              byte[] data = new byte[bytes];
              LOG.log(Level.INFO, "Sending from: " + rank + " to: " + currentPartition + " data size: " + bytes);
              System.arraycopy(allRecords, currentPartitionStart * Record.RECORD_LENGTH, data, 0, bytes);
              sorter.addData(currentPartition, data);
            }
          }
        } else {
          size[0] = (i - currentPartitionStart) * Record.RECORD_LENGTH;
          LOG.log(Level.INFO, "Sending from: " + rank + " to: " + currentPartition + " data size: " + size[0]);
          // first send the size to expect
          MPI.COMM_WORLD.send(size, 1, MPI.INT, currentPartition, 99);
          byte[] sendBuf = new byte[size[0]];
          System.arraycopy(allRecords, currentPartitionStart * Record.RECORD_LENGTH, sendBuf, 0, size[0]);
          // do send
          MPI.COMM_WORLD.send(sendBuf, size[0], MPI.BYTE, currentPartition, 99);
        }
        currentPartition = partition;
        currentPartitionStart = i;
      }
    }

    // now sort the data
    Record[] sortedRecords = sorter.sort();
    loader.save(sortedRecords);
  }

  private PartitionTree buildPartitionTree(List<Record> records) throws MPIException {
    // first create the partition communicator
    if (rank < partitionSampleNodes) {
      partitionCom = MPI.COMM_WORLD.split(0, rank);
    } else {
      partitionCom = MPI.COMM_WORLD.split(1, rank);
    }

    Record[] partitionRecords = new Record[partitionSamplesPerNode];
    for (int i = 0; i < partitionSamplesPerNode; i++) {
      partitionRecords[i] = records.get(i);
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

  public static Option createOption(String opt, boolean hasArg, String description, boolean required) {
    Option symbolListOption = new Option(opt, hasArg, description);
    symbolListOption.setRequired(required);
    return symbolListOption;
  }
}
