package com.neverwinterdp.yara.quantile;

import static org.junit.Assert.assertEquals;

import java.text.DecimalFormat;
import java.util.Random;

import org.junit.Test;

import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class QDigestUnitTest {
  static double COMPRESSION_FACTOR = 1000d ;
  
  @Test
  public void testCombination() {
    NumericDistribution dist = new NumericDistribution(); 
    long[] set1 = dist.generate(1, 10, 60000) ;
    QDigest qdigest1 = new QDigest(COMPRESSION_FACTOR); 
    qdigest1.offer(set1) ;
    
    long[] set2 = dist.generate(1, 100, 40000) ;
    QDigest qdigest2 = new QDigest(COMPRESSION_FACTOR);
    qdigest2.offer(set2) ;
    
    QDigest qdigestUnion = QDigest.unionOf(qdigest1, qdigest2) ;
    
    dist.dumpBuckets();
    
    TabularFormater  tformater = new TabularFormater("Percentile", "QDigest Combined", "Expect Value") ;
    DecimalFormat pFormater = new DecimalFormat("#.00");
    for(int i = 1; i <= 100; i++) {
      double percent = (double)i/100 ;
      tformater.addRow(
        i + "%", 
        pFormater.format(qdigestUnion.getQuantile(percent)), 
        dist.getValueByPercentile(percent)
      );
    }
    System.out.println(tformater.getFormatText());
  }
  
  @Test
  public void testSerializable() throws Exception {
    QDigest qdigest = new QDigest(COMPRESSION_FACTOR);
    Random rand = new Random() ;
    for(int i = 0; i < 10000; i++) {
      qdigest.offer(rand.nextInt((i + 1))) ;
    }
    qdigest.optimize() ;
    byte[] data = IOUtil.serialize(qdigest) ;
    QDigest qdigestClone = (QDigest) IOUtil.deserialize(data) ;
    System.out.println("Serialization Size: " + data.length)  ;
    
    byte[] customSerializedData = QDigest.serialize(qdigest) ;
    System.out.println("Custom Serialization Size: " + customSerializedData.length)  ;
    
    QDigest customqDigestClone = QDigest.deserialize(customSerializedData) ;
    assertEquals(qdigest.toString(), customqDigestClone.toString()) ;
  }
}
