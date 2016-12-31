package edu.iu.dsc.terasort;

public class ByteQuickSort {
  private byte[] numbers;
  private int number;
  private byte[] temp = new byte[Record.KEY_SIZE];

  public void sort(byte[] values) {
    // check for empty or null array
    if (values == null || values.length == 0) {
      return;
    }
    this.numbers = values;
    number = values.length / Record.KEY_SIZE;
    quickSort(0, number - 1);
  }

  private void quickSort(int low, int high) {
    int i = low, j = high;
    // Get the pivot element from the middle of the list
    int pivot = numbers[low + (high - low) / 2];

    // Divide into two lists
    while (i <= j) {
      // If the current value from the left list is smaller then the pivot
      // element then get the next element from the left list
      while (numbers[i] < pivot) {
        i++;
      }
      // If the current value from the right list is larger then the pivot
      // element then get the next element from the right list
      while (numbers[j] > pivot) {
        j--;
      }

      // If we have found a values in the left list which is larger then
      // the pivot element and if we have found a value in the right list
      // which is smaller then the pivot element then we exchange the
      // values.
      // As we are done we can increase i and j
      if (i <= j) {
        exchange(i, j);
        i++;
        j--;
      }
    }
    // Recursion
    if (low < j)
      quickSort(low, j);
    if (i < high)
      quickSort(i, high);
  }

  private void exchange(int i, int j) {
    System.arraycopy(numbers, i * Record.KEY_SIZE, temp, 0, Record.KEY_SIZE);
    System.arraycopy(numbers, getIndex(j), numbers, getIndex(j), Record.KEY_SIZE);
    System.arraycopy(temp, 0, numbers, getIndex(j), Record.KEY_SIZE);
  }

  private int getIndex(int i) {
    return i * Record.KEY_SIZE;
  }
}
