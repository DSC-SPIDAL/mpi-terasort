package edu.iu.dsc.collectives;

import mpi.MPIException;

public abstract class Collective {
  int iterations;
  int size;

  public Collective(int size, int iterations) {
    this.size = size;
    this.iterations = iterations;
  }

  public abstract void execute() throws MPIException;
}
