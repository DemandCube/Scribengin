package com.neverwinterdp.swing.console;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import com.neverwinterdp.swing.util.text.NumberFormatter;

public class JVMEnv {
  public String getMemoryInfo() {
    System.gc();
    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memory = mbean.getHeapMemoryUsage() ;

    String init = NumberFormatter.byteFormatAsHumanReadable(memory.getInit()) ;
    String max = NumberFormatter.byteFormatAsHumanReadable(memory.getMax()) ;
    String used = NumberFormatter.byteFormatAsHumanReadable(memory.getUsed()) ;
    String committed = NumberFormatter.byteFormatAsHumanReadable(memory.getCommitted()) ;
    
    return "init=" + init + ", max=" + max + ", used=" + used + ", committed=" + committed;
  }

  
}
