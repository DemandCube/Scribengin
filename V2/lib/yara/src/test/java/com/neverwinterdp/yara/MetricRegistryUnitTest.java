package com.neverwinterdp.yara;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.util.IOUtil;

public class MetricRegistryUnitTest {
  @Test
  public void testBasic() throws Exception {
    Random rand = new Random() ;
    MetricRegistry registry = new MetricRegistry("basic") ;
    registry.getPluginManager().add(new MetricPluginDummy());
    Timer timer1 = registry.getTimer("timer") ;
    assertNotNull(timer1) ;
    Timer timer2 = registry.getTimer("timer") ;
    assertEquals(timer1, timer2) ;
    for(int i = 0; i < 10000; i++) {
      timer1.update(rand.nextInt(i + 1), TimeUnit.NANOSECONDS);
    }
    
    Counter counter1 = registry.getCounter("counter") ;
    for(int i = 0; i < 100; i++) {
      counter1.incr() ;
    }
    Counter counter2 = registry.getCounter("counter") ;
    assertNotNull(counter1) ;
    assertEquals(counter1, counter2) ;
    
    byte[] data = IOUtil.serialize(registry) ;
    MetricRegistry registryClone = (MetricRegistry)IOUtil.deserialize(data) ;
    Assert.assertEquals(0, registryClone.getPluginManager().size());
    System.out.println("MetricRegistry serialization size: " + data.length);

    System.out.println("Timer serialization size: " + IOUtil.serialize(timer1).length);
    System.out.println("QDigest serialization size: " + IOUtil.serialize(timer1.getHistogram().getQDigest()).length);
    MetricPrinter mPrinter = new MetricPrinter() ;
    mPrinter.print(registry);
  }
  
  static public class MetricPluginDummy implements MetricPlugin {
    public void onTimerUpdate(String name, long timestampTick, long duration) {
    }

    public void onCounterAdd(String name, long timestampTick, long incr) {
    }

    public void onCounterDecr(String name, long timestampTick, long decr) {
    }
  }
}
