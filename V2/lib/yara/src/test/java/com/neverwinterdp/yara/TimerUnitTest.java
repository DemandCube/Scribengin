package com.neverwinterdp.yara;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.codahale.metrics.Snapshot;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.util.JSONSerializer;

public class TimerUnitTest {
  @Test
  public void testUpdate() throws Exception {
    long start = System.currentTimeMillis() ;
    Timer timer = new Timer() ;
    com.codahale.metrics.Timer codahaleTimer = new com.codahale.metrics.Timer() ;
    Random rand = new Random() ;
    int LOOP = 1000;
    for(int i = 0; i < LOOP; i++) {
      long duration = rand.nextInt(100) ;
      if(i > LOOP/5) duration = rand.nextInt(20) ;
      Thread.sleep(duration);
      timer.update(duration, TimeUnit.MICROSECONDS);
      codahaleTimer.update(duration, TimeUnit.MICROSECONDS);
    }
    byte[] data = IOUtil.serialize(timer) ;
    Timer serialization = (Timer)IOUtil.deserialize(data) ;
    String json = JSONSerializer.INSTANCE.toString(timer) ;
    //Timer  jsonSerialization = JSONSerializer.INSTANCE.fromString(json, Timer.class) ;
    TimerPrinter printer = new TimerPrinter() ;
    printer.print("NeverwinterDP", timer);
    printer.print("Serialization", serialization);
    //printer.print("JSON Serialization", jsonSerialization);
    printer.print("Codahale", codahaleTimer);
    printer.flush(); 
    
    System.out.println("In " + (System.currentTimeMillis() - start) + "ms");
    System.out.println("Timer java serialization size: " + data.length);
    System.out.println("Timer json serialization size: " + json.getBytes().length);
    System.out.println(json);
  }
  
  @Test
  public void testCombinable() throws Exception {
    Timer timer[] = new Timer[6] ;
    com.codahale.metrics.Timer[] cTimer = new com.codahale.metrics.Timer[timer.length] ;
    for(int i = 0; i < timer.length; i++) {
      timer[i] = new Timer("timer") ;
      cTimer[i] = new com.codahale.metrics.Timer() ;
    }
    Timer timerAll = new Timer("timer.all") ;
    com.codahale.metrics.Timer cTimerAll = new com.codahale.metrics.Timer() ;
    Random rand = new Random() ;
    int LOOP = 1000;
    for(int i = 0; i < LOOP; i++) {
      int mod = i % timer.length ;
      long duration = rand.nextInt(5 + mod*5) ;
      timer[mod].update(duration, TimeUnit.MICROSECONDS);
      cTimer[mod].update(duration, TimeUnit.MICROSECONDS);
      timerAll.update(duration, TimeUnit.MICROSECONDS);
      cTimerAll.update(duration, TimeUnit.MICROSECONDS);
      Thread.sleep(duration);
    }
    TimerPrinter printer = new TimerPrinter() ;
    for(int i = 0; i < timer.length; i++) {
      int idx = i + 1;
      printer.print("Timer " + idx, timer[i]);
      printer.print("C Timer " + idx, cTimer[i]);
    }
    printer.print("Timer All", timerAll);
    printer.print("C Timer All", cTimerAll);
    printer.print("Timer 1.." + timer.length, Timer.combine(timer));
    printer.flush();
  }
  
  static public class TimerPrinter extends MetricPrinter.TimerPrinter {
    public void print(String name, com.codahale.metrics.Timer timer) {
      Snapshot snapshot = timer.getSnapshot() ;
      tformater.addRow(
        name, 
        timer.getCount(),
        
        snapshot.getMin(), snapshot.getMax(), dFormater.format(snapshot.getMean()), dFormater.format(snapshot.getStdDev()),
        
        dFormater.format(snapshot.getValue(0.75)),
        dFormater.format(snapshot.getValue(0.90)),
        dFormater.format(snapshot.getValue(0.95)),
        dFormater.format(snapshot.getValue(0.99)),
        dFormater.format(snapshot.getValue(0.99999)),
        
        dFormater.format(timer.getOneMinuteRate()), 
        dFormater.format(timer.getFiveMinuteRate()), 
        dFormater.format(timer.getFifteenMinuteRate()),
        dFormater.format(timer.getMeanRate())
      );
    }
    
    public void flush() {
      System.out.println(tformater.getFormatText()) ;
    }
  }
}
