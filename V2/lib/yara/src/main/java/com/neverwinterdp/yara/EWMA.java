package com.neverwinterdp.yara;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

/**
 * An exponentially-weighted moving average.
 *
 * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">UNIX Load Average Part 1: How
 *      It Works</a>
 * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">UNIX Load Average Part 2: Not
 *      Your Average Average</a>
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class EWMA implements Serializable {
  private static final int INTERVAL = 5;
  private static final double SECONDS_PER_MINUTE = 60.0;
  private static final int ONE_MINUTE = 1;
  private static final int FIVE_MINUTES = 5;
  private static final int FIFTEEN_MINUTES = 15;
  private static final double M1_ALPHA = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / ONE_MINUTE);
  private static final double M5_ALPHA = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / FIVE_MINUTES);
  private static final double M15_ALPHA = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / FIFTEEN_MINUTES);

  private volatile boolean    initialized        = false;
  private volatile double     rate               = 0.0;

  private final AtomicLong    uncounted          = new AtomicLong();
  private final double        alpha, interval;

  /**
   * Creates a new EWMA which is equivalent to the UNIX one minute load average and which expects
   * to be ticked every 5 seconds.
   *
   * @return a one-minute EWMA
   */
  public static EWMA oneMinuteEWMA() {
    return new EWMA(M1_ALPHA, INTERVAL, TimeUnit.SECONDS);
  }

  /**
   * Creates a new EWMA which is equivalent to the UNIX five minute load average and which expects
   * to be ticked every 5 seconds.
   *
   * @return a five-minute EWMA
   */
  public static EWMA fiveMinuteEWMA() {
    return new EWMA(M5_ALPHA, INTERVAL, TimeUnit.SECONDS);
  }

  /**
   * Creates a new EWMA which is equivalent to the UNIX fifteen minute load average and which
   * expects to be ticked every 5 seconds.
   *
   * @return a fifteen-minute EWMA
   */
  public static EWMA fifteenMinuteEWMA() {
    return new EWMA(M15_ALPHA, INTERVAL, TimeUnit.SECONDS);
  }

  /**
   * Create a new EWMA with a specific smoothing constant.
   *
   * @param alpha        the smoothing constant
   * @param interval     the expected tick interval
   * @param intervalUnit the time unit of the tick interval
   */
  public EWMA(double alpha, long interval, TimeUnit intervalUnit) {
    this.interval = intervalUnit.toNanos(interval);
    this.alpha = alpha;
  }
  
  public EWMA(boolean intialized, double rate, AtomicLong uncounted, double alpha, double interval) {
    this.initialized = initialized ;
    this.rate = rate ;
    this.uncounted.set(uncounted.longValue()); ;
    this.interval = interval;
    this.alpha = alpha;
  }

  /**
   * Update the moving average with a new value.
   *
   * @param n the new value
   */
  public void update(long n) {
    uncounted.addAndGet(n);
  }

  /**
   * Mark the passage of time and decay the current rate accordingly.
   */
  public void tick() {
    final long count = uncounted.getAndSet(0) ;;
    final double instantRate = count / interval;
    if (initialized) {
      rate += (alpha * (instantRate - rate));
    } else {
      rate = instantRate;
      initialized = true;
    }
  }

  /**
   * Returns the rate in the given units of time.
   *
   * @param rateUnit the unit of time
   * @return the rate
   */
  public double getRate(TimeUnit rateUnit) {
    return rate * (double) rateUnit.toNanos(1);
  }
  
  static public EWMA unionOf(EWMA e1, EWMA e2) {
    if(e1.alpha != e2.alpha) {
      throw new java.lang.IllegalStateException("Expect same alpha value for EWMA union") ;
    }
    if(e1.interval != e2.interval) {
      throw new java.lang.IllegalStateException("Expect same interval value for EWMA union") ;
    }
    boolean    initialized  = e1.initialized || e2.initialized ;
    double     rate  = e1.rate + e2.rate ;
    AtomicLong uncounted          = new AtomicLong(e1.uncounted.longValue() + e2.uncounted.longValue());
    double        alpha = e1.alpha, interval = e1.interval;
    
    return new EWMA(initialized, rate, uncounted, alpha, interval) ;
  }
}
