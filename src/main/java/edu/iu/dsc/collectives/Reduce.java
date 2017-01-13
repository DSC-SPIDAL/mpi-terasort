package edu.iu.dsc.collectives;

import mpi.MPI;
import mpi.MPIException;

import java.nio.IntBuffer;

public class Reduce extends Collective {
  public Reduce(int size, int iterations) {
    super(size, iterations);
  }

  @Override
  public void execute() throws MPIException {
    IntBuffer intBuffer = MPI.newIntBuffer(size);
    IntBuffer receiveBuffer = MPI.newIntBuffer(size);
    for (int i = 0; i < size; i++) {
      intBuffer.put(i);
    }

    for (int i = 0; i < iterations; i++) {
      MPI.COMM_WORLD.reduce(intBuffer, receiveBuffer, size, MPI.INT, MPI.SUM, 0);
    }
  }
}
