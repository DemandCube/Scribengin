package com.neverwinterdp.scribengin.nizarS3.partition;

import static com.google.common.math.LongMath.divide;
import static java.math.RoundingMode.UP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.util.Label;

public class OffsetPartitionUnitTest {

  private final int offsetPerPartition = 5;
  private OffsetPartitioner offsetPartitioner;

  @Before
  public void setUp() {
    offsetPartitioner = new OffsetPartitioner(offsetPerPartition );
  }

  // get partition for startOffset=0 and endOffset=100
  @Test
  @Label("S3_15")
  public void testPartitioner() {
    long startOffset = 995;
    long endOffset = 999;
    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
    
    String expected =folder +
        "/" + startOffset + "_" + endOffset;



    System.out.println("expected " + expected);
    assertEquals(expected, offsetPartitioner.getPartition(startOffset, endOffset));
  }

  @Test
  @Label("S3_15")
  public void testPartitioner2() {
    long startOffset = 5;
    long endOffset = 9;
    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
      String expected = folder +
        "/" + startOffset + "_" + endOffset;

    assertEquals(expected, offsetPartitioner.getPartition(startOffset, endOffset));
  }

  @Test(expected = IllegalArgumentException.class)
  @Label("S3_15")
  public void testPartitioner3() {
    long startOffset = 0;
    long endOffset = 5;
    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
    String expected = folder + "/" + startOffset + "_" + endOffset;
    assertEquals(expected, offsetPartitioner.getPartition(startOffset, endOffset));
  }

  @Test
  @Label("S3_16")
  public void testPartitionerOffsets() {
    long startOffset = 100;
    long endOffset = 104;
    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
     String expected = folder +
        "/" + startOffset + "_" + endOffset;

    assertEquals(expected, offsetPartitioner.getPartition(startOffset, endOffset));
  }

  @Test
  @Label("S3_17")
  public void testPartitionerOffsets1000() {
    long startOffset = 1000;
    long endOffset = 1004;
    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
      String expected = folder +
        "/" + startOffset + "_" + endOffset;

    assertEquals(expected, offsetPartitioner.getPartition(startOffset, endOffset));
  }

  @Test
  @Label("S3_18")
  public void testIncompleteOffsetRange() {
    long startOffset = 0;
    long endOffset = 3;
    long folder = offsetPerPartition * divide(endOffset, offsetPerPartition, UP);
     String expected = folder +
        "/" + startOffset + "_" + endOffset;

    assertEquals(expected, offsetPartitioner.getPartition(startOffset, endOffset));
  }

  @Test(expected = IllegalArgumentException.class)
  @Label("S3_1999")
  public void testStartOffsetLessThanEndOffset() {
    int startOffset = 10;
    int endOffset = 0;
    offsetPartitioner.getPartition(startOffset, endOffset);

    fail("We should never get here.");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStartOffsetEqualToEndOffset() {
    int startOffset = 10;
    int endOffset = 10;
    offsetPartitioner.getPartition(startOffset, endOffset);

    fail("We should never get here.");
  }
}