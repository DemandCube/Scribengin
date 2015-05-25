package com.neverwinterdp.buffer.chronicle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;

import org.junit.Test;

import com.neverwinterdp.util.FileUtil;

public class VanillaChronicleUnitTest {
  @Test
  public void testMultipleCycles2() throws Exception {
    final String baseDir = "build/vanilla";
    assertNotNull(baseDir);
    FileUtil.removeIfExist(baseDir, false);

    // Create with small data and index sizes so that the test frequently
    // generates new files
    VanillaChronicleConfig config = new VanillaChronicleConfig();
    config.cycleFormat("yyyy-MM-dd-HH:mm:ss");
    config.cycleLength(60 * 1000, false);
    config.entriesPerCycle(512 * 1024);
    config.dataBlockSize(4 * 1024 * 1024);
    config.indexBlockSize(1 * 1024 * 1024);
    final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
    chronicle.clear();
    
    try {
      ExcerptAppender appender = chronicle.createAppender();
      long start = System.currentTimeMillis() ;
      for(int j = 0; j < 4500000; j++) {
        appender.startExcerpt();
        appender.writeInt(j + 1);
        appender.finish();
      }
      long exec = System.currentTimeMillis() - start ;
      System.out.println("Append in " + exec + "ms");
      
      
      for(int i = 0; i < 5; i++) {
        long sum = 0 ;
        AtomicInteger count = new AtomicInteger() ;
        ExcerptTailer tailer     = chronicle.createTailer();
        int lastValue = 0 ;
        while(tailer.nextIndex()) {
          count.incrementAndGet() ;
          lastValue = tailer.readInt();
          sum += lastValue;
          tailer.finish();
        }
        //tailer.flush() ;
        tailer.close();
        System.out.println("files = " +tailer.file()) ;
        System.out.println(
            "count = " + count.get() + ", sum = " + sum + ", last value = " + lastValue + ", last index = " + chronicle.lastIndex() + 
            ", was padding = " + tailer.wasPadding() + ", nextIndex = " + tailer.nextIndex() + 
            ", finished = " + tailer.isFinished());
      }
      
      appender.close();
      chronicle.checkCounts(1, 1);
    } finally {
      chronicle.close();
    }
  }
  
  @Test
  public void testTailerToEnd1() throws IOException {
    final String baseDir = "build/vanilla";
    assertNotNull(baseDir);

    final VanillaChronicle chronicle = new VanillaChronicle(baseDir);
    chronicle.clear();

    try {
      ExcerptAppender appender = chronicle.createAppender();
      for (long i = 0; i < 3; i++) {
        appender.startExcerpt();
        appender.writeLong(i);
        appender.finish();
      }
      appender.close();
      // test a vanilla tailer, wind to end
      ExcerptTailer tailer = chronicle.createTailer().toEnd();
      assertEquals(2, tailer.readLong());
      assertFalse(tailer.nextIndex());

      tailer.close();

      chronicle.checkCounts(1, 1);
    } finally {
      chronicle.close();
      chronicle.clear();
    }
  }
  
  /*
   * TODO Fix this test as it sometime jumps e.g.
   * 
   * Major: 1396197284 Major: 1396197285
   * 
   * java.lang.AssertionError: major jumped Expected :1396197286 Actual
   * :1396197287
   */
  @Test
  public void testReplicationWithRollingFilesEverySecond() throws Exception {
    // TODO int RUNS = 100000;
    final int RUNS = 5 * 1000;

    final String baseDir = "build/vanilla";
    assertNotNull(baseDir);

    final VanillaChronicleConfig config = new VanillaChronicleConfig();
    config.entriesPerCycle(1L << 16);
    config.cycleLength(1000, false);
    config.cycleFormat("yyyyMMddHHmmss");
    config.indexBlockSize(16L << 10);

    final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
    chronicle.clear();
    try {
      ExcerptAppender appender = chronicle.createAppender();
      ExcerptTailer tailer = chronicle.createTailer();
      long lastMajor = 0;
      for (int i = 0; i < RUNS; i++) {
        appender.startExcerpt();
        long value = 1000000000 + i;
        appender.append(value).append(' ');
        appender.finish();
        // System.out.println("Sleeping " +i );
        Thread.sleep(1);
        assertTrue(tailer.nextIndex());
        long major = tailer.index() / config.entriesPerCycle();
        if (lastMajor == 0 || lastMajor == major) {
          // ok.
        } else if (lastMajor + 1 == major) {
          System.out.println("Major: " + major);
        } else {
          assertEquals("major jumped", lastMajor + 1, major);
        }
        lastMajor = major;
        // System.out.printf("Index: %x%n", major);
        assertEquals("i: " + i, value, tailer.parseLong());
        assertEquals("i: " + i, 0, tailer.remaining());
        tailer.finish();
      }

      appender.close();
      tailer.close();

      chronicle.checkCounts(1, 1);
    } finally {
      chronicle.close();
      //chronicle.clear();
      //assertFalse(new File(baseDir).exists());
    }
  }
}
