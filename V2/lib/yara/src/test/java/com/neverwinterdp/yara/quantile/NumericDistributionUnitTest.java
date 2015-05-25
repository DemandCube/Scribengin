package com.neverwinterdp.yara.quantile;

import org.junit.Test;

public class NumericDistributionUnitTest {
  static double COMPRESSION_FACTOR = 1000d ;
  static int[] NUMBER_SET = {1, 0, 0, 1, 1, 0, 1, 1, 1 , 1, 0, 2, 3, 20, 21, 22, 21, 20, 30};
  
  @Test
  public void testNumericDistribution() {
    NumericDistribution dist = new NumericDistribution(); 
    dist.add(NUMBER_SET) ;
    dist.dumpBuckets();
  }
}