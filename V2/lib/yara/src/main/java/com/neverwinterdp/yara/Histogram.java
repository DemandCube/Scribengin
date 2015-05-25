package com.neverwinterdp.yara;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.yara.quantile.QDigest;

/**
 * A metric which calculates the distribution of a value.
 *
 * @see <a href="http://www.johndcook.com/standard_deviation.html">Accurately computing running
 *      variance</a>
 */
public class Histogram  implements Serializable {
  static double COMPRESSION_FACTOR = 150d ;

  private final QDigest qdigest;
  private final AtomicLong count;

  /**
   * Creates a new {@link Histogram} with the given reservoir.
   *
   * @param reservoir the reservoir to create a histogram from
   */
  public Histogram() {
    this.qdigest = new QDigest(COMPRESSION_FACTOR);
    this.count = new AtomicLong() ;
  }
  
  public Histogram(QDigest qdigest, AtomicLong count) {
    this.qdigest = qdigest;
    this.count   = count ;
  }

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  public void update(int value) {
    update((long) value);
  }

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  public void update(long value) {
    count.incrementAndGet() ;
    qdigest.offer(value);
  }

  /**
   * Returns the number of values recorded.
   * @return the number of values recorded
   */
  public long getCount() { return count.longValue() ; }
  
  public long getMin() { return qdigest.getQuantile(0) ; }
  
  public long getMax() { return qdigest.getQuantile(1.0) ; }
  
  public double getMean() { return qdigest.getMean() ; }
  
  public double getStdDev() { return qdigest.getStdDev() ; }
  
  public long getQuantile(double percent) { return qdigest.getQuantile(percent) ; }
  
  public QDigest getQDigest() { return this.qdigest ; }
  
  static public Histogram unionOf(Histogram h1, Histogram h2) {
    QDigest qdigest = QDigest.unionOf(h1.qdigest, h2.qdigest) ;
    AtomicLong count = new AtomicLong() ;
    count.addAndGet(h1.count.longValue()) ;
    count.addAndGet(h2.count.longValue()) ;
    return new Histogram(qdigest, count) ;
  }
}
