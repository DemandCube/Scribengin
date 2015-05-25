package com.neverwinterdp.yara.quantile;

import java.text.DecimalFormat;
import java.util.Random;

import org.junit.Test;

import com.clearspring.analytics.stream.quantile.QDigest;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Snapshot;
import com.neverwinterdp.util.text.TabularFormater;

public class AlgorithmComparatorUnitTest {
  static double COMPRESSION_FACTOR = 1000d ;
  static long[] NUMBER_SET = {1, 0, 0, 1, 1, 0, 1, 1, 1 , 1, 0, 2, 3, 20, 21, 22, 21, 20, 30};
  
  @Test
  public void testAlgorithms() {
    NumericDistribution dist = new NumericDistribution() ;
    dist.add(NUMBER_SET);
    testAlgorithms("Basic test for a small number set", dist) ;
    
    int FACTOR = 10000 ;
    dist = new NumericDistribution() ;
    dist.generate(1, 20,  4 * FACTOR) ;
    dist.generate(1, 40,  3 * FACTOR) ;
    dist.generate(1, 60,  2 * FACTOR) ;
    dist.generate(1, 100, 1 * FACTOR) ;
    String title = 
        "Test for a large number set\n" +
        "Generate a set of number range from 1 - 100 where at least 60% is smaller than 20" ;
    testAlgorithms(title, dist) ;
  }
  
  void testAlgorithms(String title, NumericDistribution dist) {
    System.out.println("****************************************************************************");
    System.out.println(title);
    System.out.println("****************************************************************************");
    dist.dumpBuckets();
    long[] numberSet = dist.getNumericSet() ;
    AlgorithmRunner qdigestRunner = new QDigestAlgorithmRunner() ;
    qdigestRunner.run(numberSet);
    qdigestRunner.printExecuteTime("QDigest");

    AlgorithmRunner neverwinterdpQDigestRunner = new NeverwinterDPQDigestAlgorithmRunner() ;
    neverwinterdpQDigestRunner.run(numberSet);
    neverwinterdpQDigestRunner.printExecuteTime("NeverwinterDP QDigest");

    
    AlgorithmRunner tdigestRunner = new TDigestAlgorithmRunner() ;
    tdigestRunner.run(numberSet);
    tdigestRunner.printExecuteTime("TDigest");

    AlgorithmRunner codahaleRunner = new CodahaleReservoirAlgorithmRunner() ;
    codahaleRunner.run(numberSet);
    codahaleRunner.printExecuteTime("Codahale Reservoid");
    
    TabularFormater  tformater = new TabularFormater("Percentile", "QDigest", "N QDigest", "TDigest", "Codahale Reservoir", "Expect Value") ;
    DecimalFormat pFormater = new DecimalFormat("#.00");
    for(int i = 1; i <= 100; i++) {
      double percent = (double)i/100 ;
      tformater.addRow(
        i + "%", 
        pFormater.format(qdigestRunner.getQuantile(percent)), 
        pFormater.format(neverwinterdpQDigestRunner.getQuantile(percent)), 
        pFormater.format(tdigestRunner.getQuantile(percent)),
        pFormater.format(codahaleRunner.getQuantile(percent)),
        dist.getValueByPercentile(percent)
      );
    }
    System.out.println(tformater.getFormatText());
    System.out.println("\n\n\n");
  }
  
  static abstract public class AlgorithmRunner {
    private long executeTime = 0;
    private int  numberOfNumbers ;
    
    public void run(long[] numbers) {
      this.numberOfNumbers = numbers.length ;
      long startTime = System.currentTimeMillis() ;
      runAlgorithm(numbers) ;
      executeTime = System.currentTimeMillis() - startTime ;
    }
    
    public long getExecuteTime() { return  executeTime ; }
    
    public void printExecuteTime(String alg) {
      System.out.println("Algorithm " + alg + " is executed in " + executeTime + "ms for a set of " + numberOfNumbers + " numbers");
    }
    
    abstract public double getQuantile(double percent) ;
    
    abstract protected void runAlgorithm(long[] numbers) ;
  }
  
  static public class QDigestAlgorithmRunner extends AlgorithmRunner {
    private QDigest qdigest ;
    
    public double getQuantile(double percent) {
      return qdigest.getQuantile(percent);
    }

    protected void runAlgorithm(long[] numbers) {
      qdigest = new QDigest(COMPRESSION_FACTOR); 
      for(int i = 0; i < numbers.length; i++) {
        qdigest.offer(numbers[i]);
      }
    }
  }
  
  static public class NeverwinterDPQDigestAlgorithmRunner extends AlgorithmRunner {
    private com.neverwinterdp.yara.quantile.QDigest qdigest ;
    
    public double getQuantile(double percent) {
      return qdigest.getQuantile(percent);
    }

    protected void runAlgorithm(long[] numbers) {
      qdigest = new com.neverwinterdp.yara.quantile.QDigest(COMPRESSION_FACTOR); 
      for(int i = 0; i < numbers.length; i++) {
        qdigest.offer(numbers[i]);
      }
    }
  }
  
  static public class TDigestAlgorithmRunner extends AlgorithmRunner {
    TDigest tdigest ;
    
    public double getQuantile(double percent) {
      return tdigest.quantile(percent);
    }

    protected void runAlgorithm(long[] numbers) {
      tdigest = new TDigest(COMPRESSION_FACTOR, new Random());
      for(int i = 0; i < numbers.length; i++) {
        tdigest.add(numbers[i]);
      }
    }
  }
  
  static public class CodahaleReservoirAlgorithmRunner extends AlgorithmRunner {
    ExponentiallyDecayingReservoir reservoir ;
    Snapshot snapshot ;
    
    public double getQuantile(double percent) {
      return snapshot.getValue(percent);
    }

    protected void runAlgorithm(long[] numbers) {
      reservoir = new ExponentiallyDecayingReservoir() ;
      for(int i = 0; i < numbers.length; i++) {
        reservoir.update(numbers[i]);
      }
      snapshot = reservoir.getSnapshot() ;
    }
  }
}