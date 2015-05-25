package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.yara.Histogram;
import com.neverwinterdp.yara.Timer;

public class TimerSnapshot implements Serializable {
  private long   count;
  private long   min;
  private long   max;
  private long   mean;
  private long   stddev;
  private long   p50;
  private long   p75;
  private long   p90;
  private long   p95;
  private long   p98;
  private long   p99;
  private long   p999;
  private double m1Rate;
  private double m5Rate;
  private double m15Rate;
  private double meanRate;
  private String durationUnit = "ns";
  
  public TimerSnapshot() {
  }

  public TimerSnapshot(Timer timer, TimeUnit timeUnit) {
    Histogram histogram = timer.getHistogram();
    count = timer.getCount();
    min = timeUnit.convert(histogram.getMin(), TimeUnit.NANOSECONDS);
    max = timeUnit.convert(histogram.getMax(), TimeUnit.NANOSECONDS);
    mean = timeUnit.convert((long)histogram.getMean(), TimeUnit.NANOSECONDS);
    stddev = timeUnit.convert((long)histogram.getStdDev(), TimeUnit.NANOSECONDS);
    p50 = timeUnit.convert(histogram.getQuantile(0.50), TimeUnit.NANOSECONDS);
    p75 = timeUnit.convert(histogram.getQuantile(0.75), TimeUnit.NANOSECONDS);
    p90 = timeUnit.convert(histogram.getQuantile(0.90), TimeUnit.NANOSECONDS);
    p95 = timeUnit.convert(histogram.getQuantile(0.95), TimeUnit.NANOSECONDS);
    p98 = timeUnit.convert(histogram.getQuantile(0.98), TimeUnit.NANOSECONDS);
    p99 = timeUnit.convert(histogram.getQuantile(0.99), TimeUnit.NANOSECONDS);
    p999 = timeUnit.convert(histogram.getQuantile(0.999), TimeUnit.NANOSECONDS);
    m1Rate = timer.getOneMinuteRate();
    m5Rate = timer.getFiveMinuteRate();
    m15Rate = timer.getFifteenMinuteRate();
    meanRate = timer.getMeanRate();
    durationUnit = timeUnit.toString() ;
  }

  public long getCount() { return count; }

  public long getMin() { return min; }

  public long getMax() { return max; }

  public double getMean() { return mean; }

  public double getStddev() { return stddev; }

  public double getP50() { return p50; }
  
  public double getP75() { return p75; }

  public double getP90() { return p90; }

  public double getP95() { return p95; }

  public double getP98() { return p98; }
  
  public double getP99() { return p99; }

  public double getP999() { return p999; }

  public double getM1Rate() { return m1Rate; }

  public double getM5Rate() { return m5Rate; }

  public double getM15Rate() { return m15Rate; }

  public double getMeanRate() { return meanRate; }
  
  public String getDurationUnits() { return durationUnit; }
  
  public String getRateUnits() { return "call/s"; }
}