package com.neverwinterdp.yara;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A meter metric which measures mean throughput and one-, five-, and fifteen-minute
 * exponentially-weighted moving average throughputs.
 *
 * @see EWMA
 */
public class Meter implements Serializable {
  private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);

  private final EWMA        m1Rate;
  private final EWMA        m5Rate;
  private final EWMA        m15Rate;

  private final AtomicLong  count ;
  private final long        startTime;
  private final AtomicLong  lastTick;

  /**
   * Creates a new {@link Meter}.
   *
   * @param clock      the clock to use for the meter ticks
   */
  public Meter() {
    count         = new AtomicLong();
    startTime = Clock.defaultClock().getTick();
    lastTick = new AtomicLong(startTime);
    
    m1Rate        = EWMA.oneMinuteEWMA() ;
    m5Rate        = EWMA.fiveMinuteEWMA();
    m15Rate       = EWMA.fifteenMinuteEWMA();
  }
  
  public Meter(AtomicLong  count, long startTime, AtomicLong lastTick,
               EWMA m1Rate, EWMA m5Rate, EWMA m15Rate) {
    this.count     = count ;
    this.startTime = startTime;
    this.lastTick  = lastTick;
    
    this.m1Rate        = m1Rate ;
    this.m5Rate        = m5Rate;
    this.m15Rate       = m15Rate;
  }

  /**
   * Mark the occurrence of an event.
   * @return  return the timestamp
   */
  public long mark() {
    return mark(1);
  }

  /**
   * Mark the occurrence of a given number of events.
   *
   * @param n the number of events
   * @return timestamp 
   */
  public long mark(long n) {
    long timestampTick = Clock.defaultClock().getTick() ; 
    mark(timestampTick, n);
    return timestampTick ;
  }
  
  /**
   * Mark the occurrence of a given number of events.
   *
   * @param n the number of events
   */
  public long mark(long timestampTick, long n) {
    tickIfNecessary(timestampTick);
    count.addAndGet(n);
    m1Rate.update(n);
    m5Rate.update(n);
    m15Rate.update(n);
    return timestampTick ;
  }

  
  private void tickIfNecessary(long timestampTick) {
    final long oldTick = lastTick.get();
    final long age = timestampTick - oldTick;
    if (age > TICK_INTERVAL) {
      final long newIntervalStartTick = timestampTick - age % TICK_INTERVAL;
      if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
        final long requiredTicks = age / TICK_INTERVAL;
        for (long i = 0; i < requiredTicks; i++) {
          m1Rate.tick();
          m5Rate.tick();
          m15Rate.tick();
        }
      }
    }
  }

  
  public long getCount() {
    return count.longValue();
  }
  
  public double getFifteenMinuteRate() {
    tickIfNecessary(Clock.defaultClock().getTick());
    return m15Rate.getRate(TimeUnit.SECONDS);
  }

  public double getFiveMinuteRate() {
    tickIfNecessary(Clock.defaultClock().getTick());
    return m5Rate.getRate(TimeUnit.SECONDS);
  }

  public double getMeanRate() {
    if (getCount() == 0) {
      return 0.0;
    } else {
      final double elapsed = (Clock.defaultClock().getTick() - startTime);
      return getCount() / elapsed * TimeUnit.SECONDS.toNanos(1);
    }
  }

  public double getOneMinuteRate() {
    tickIfNecessary(Clock.defaultClock().getTick());
    return m1Rate.getRate(TimeUnit.SECONDS);
  }
  
  static public Meter unionOf(Meter m1, Meter m2) {
    AtomicLong  count         = new AtomicLong(m1.count.longValue() + m2.count.longValue());
    long        startTime     = m1.startTime < m2.startTime ? m1.startTime : m2.startTime;
    AtomicLong  lastTick = 
      new AtomicLong(m1.lastTick.longValue() > m2.lastTick.longValue() ? m1.lastTick.longValue() : m2.lastTick.longValue());
    
    EWMA m1Rate   = EWMA.unionOf(m1.m1Rate, m2.m1Rate) ;
    EWMA m5Rate   = EWMA.unionOf(m1.m5Rate, m2.m5Rate) ;
    EWMA m15Rate  = EWMA.unionOf(m1.m15Rate, m2.m15Rate) ;
    
    Meter meter = new Meter(count, startTime, lastTick, m1Rate, m5Rate, m15Rate) ;
    return meter ;
  }
}