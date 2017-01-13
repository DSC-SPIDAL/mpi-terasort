package edu.iu.dsc.collectives;

import mpi.MPI;
import mpi.MPIException;

import java.nio.IntBuffer;

public class BroadCast extends Collective {
  public BroadCast(int size, int iterations) {
    super(size, iterations);
  }

  @Override
  public void execute() throws MPIException {
    IntBuffer intBuffer = MPI.newIntBuffer(size);
    for (int i = 0; i < size; i++) {
      intBuffer.put(i);
    }

    for (int i = 0; i < iterations; i++) {
      MPI.COMM_WORLD.bcast(intBuffer, size, MPI.INT, 0);
    }
  }
}
