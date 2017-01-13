package edu.iu.dsc.collectives;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Program {
  private static Logger LOG = Logger.getLogger(Program.class.getName());
  // the collective to test
  int collective;

  // size of the data to test
  int dataSize;

  // number of iterations to use
  int iterations;

  private void readProgramArgs(String []args) {
    Options options = new Options();
    options.addOption("collective", true, "Type of collective, 0 Reduce, 1 All Reduce, 2 Broad cast");
    options.addOption("size", true, "Size of data to use");
    options.addOption("itr", true, "Number of iterations");

    CommandLineParser commandLineParser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = commandLineParser.parse(options, args);
      collective = Integer.parseInt(cmd.getOptionValue("collective"));
      dataSize = Integer.parseInt(cmd.getOptionValue("size"));
      iterations = Integer.parseInt(cmd.getOptionValue("itr"));
    } catch (ParseException e) {
      LOG.log(Level.SEVERE, "Failed to read the options", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("program", options);
      throw new RuntimeException(e);
    }
  }

  public void execute(String []args) throws MPIException {
    readProgramArgs(args);
    System.out.println(String.format("Collective %d with size %d and iteration %d", collective, dataSize, iterations));
    long startTime = System.currentTimeMillis();
    if (collective == 0) {
      Reduce r = new Reduce(dataSize, iterations);
      r.execute();
    } else if (collective == 1) {
      AllReduce a = new AllReduce(dataSize, iterations);
      a.execute();
    } else if (collective == 2) {
      BroadCast b = new BroadCast(dataSize, iterations);
      b.execute();
    }

    long endTime = System.currentTimeMillis();
    long time = endTime - startTime;
    int rank = MPI.COMM_WORLD.getRank();
    if (rank == 0) {
      System.out.println(String.format("Collective %d with size %d and iteration %d took %d", collective, dataSize, iterations, time));
    }
  }

  public static void main(String[] args) {
    try {
      MPI.Init(args);
      // execute the program
      Program p = new Program();
      p.execute(args);
      MPI.Finalize();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to tear down MPI");
    }
  }
}
