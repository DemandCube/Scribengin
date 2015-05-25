package com.neverwinterdp.yara;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class Timer implements Serializable {
  transient private MetricPlugin metricPlugin ;
  
  private String name ;
  private Meter   meter;
  private Histogram histogram ;
  
  public Timer() {
    histogram = new Histogram();
    meter = new Meter();
  }
  
  public Timer(String name) {
    this() ;
    this.name = name ;
  }
  
  public Timer(String name, Histogram histogram, Meter meter) {
    this.name = name ;
    this.histogram = histogram;
    this.meter = meter;
  }
  
  public String getName() { return this.name ; }
  
  public void setMetricPlugin(MetricPlugin plugin) {
    this.metricPlugin = plugin ;
  }
  /**
   * Adds a recorded duration.
   *
   * @param  duration the length of the duration
   * @param  unit     the scale unit of {@code duration}
   * @return the update timestamp
   */
  public long update(long duration, TimeUnit unit) {
    return update(unit.toNanos(duration));
  }

  public long update(long timestamp, long duration, TimeUnit unit) {
    return update(unit.toNanos(duration));
  }
  
  /**
   * Adds a recorded duration.
   *
   * @param  duration the length of the duration in nano second
   * @return the update timestamp
   */
  public long update(long duration) {
    return update(Clock.defaultClock().getTick(), duration);
  }
  
  synchronized public long update(long timestampTick, long duration) {
    if (duration >= 0) {
      histogram.update(duration);
      timestampTick = meter.mark(timestampTick,1l);
      if(metricPlugin != null) {
        metricPlugin.onTimerUpdate(name, timestampTick, duration);
      }
      return timestampTick ;
    }
    return -1;
  }

  /**
   * Returns a new {@link Context}.
   * @return a new {@link Context}
   * @see Context
   */
  public Context time() { return new Context(this); }

  public long getCount() { return histogram.getCount(); }

  public Histogram getHistogram() { return this.histogram ; }
  
  public double getOneMinuteRate() { return meter.getOneMinuteRate(); }
  
  public double getFiveMinuteRate() { return meter.getFiveMinuteRate(); }

  public double getFifteenMinuteRate() { return meter.getFifteenMinuteRate(); }

  public double getMeanRate() { return meter.getMeanRate(); }

  static public Timer unionOf(Timer timer1, Timer timer2) {
    String name = timer1.getName() ;
    if(name == null || !name.equals(timer2.getName())) {
      throw new RuntimeException("timer name is null or not equals") ;
    }
    Histogram histogram = Histogram.unionOf(timer1.histogram, timer2.histogram) ;
    Meter meter = Meter.unionOf(timer1.meter, timer2.meter) ;
    return new Timer(name, histogram, meter) ;
  }
  
  static public Timer combine(Timer ... timer) {
    if(timer.length == 0) return new Timer() ;
    else if(timer.length == 1) return timer[0] ;
    Timer combine = Timer.unionOf(timer[0], timer[1]) ;
    for(int i = 2; i < timer.length; i++) {
      combine = Timer.unionOf(combine, timer[i]) ;
    }
    return combine ;
  }
  
  static public Timer combine(Collection<Timer> timers) {
    Timer[] array = new Timer[timers.size()] ;
    timers.toArray(array) ;
    return Timer.combine(array) ;
  }
  
  /**
   * A timing context.
   * @see Timer#time()
   */
  public static class Context implements Closeable {
    private final Timer timer;
    private final long startTime;

    private Context(Timer timer) {
      this.timer = timer;
      this.startTime = Clock.defaultClock().getTick();
    }

    /**
     * Updates the timer with the difference between current and start time. Call to this method will
     * not reset the start time. Multiple calls result in multiple updates.
     * @return the elapsed time in nanoseconds
     */
    public long stop() {
      final long elapsed = Clock.defaultClock().getTick() - startTime;
      timer.update(elapsed, TimeUnit.NANOSECONDS);
      return elapsed;
    }

    /** Equivalent to calling {@link #stop()}. */
    @Override
    public void close() { stop(); }
  }
}
