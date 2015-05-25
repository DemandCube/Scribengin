package com.neverwinterdp.util.jvm;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;

public class DetailThreadInfo implements Serializable{
  private String threadStartedCount;
  private String threadPeakCount;
  private String threadCount;
  private String threadDeamonCount;
  
  private ThreadMXBean mbean;
  private ArrayList<ThreadInfo> threadinfos;

  public DetailThreadInfo() {
    mbean = ManagementFactory.getThreadMXBean();
    threadinfos = new ArrayList<ThreadInfo>();
    
    threadStartedCount = Long.toString(mbean.getTotalStartedThreadCount());
    threadPeakCount = Long.toString(mbean.getPeakThreadCount());
    threadCount = Long.toString(mbean.getThreadCount());
    threadDeamonCount = Long.toString(mbean.getDaemonThreadCount());
  }
  
  public ArrayList<ThreadInfo> getAllThreadInfo(){
    long[] tid = mbean.getAllThreadIds() ;
    for(int i = 0; i < tid.length; i++) {
      threadinfos.add(mbean.getThreadInfo(tid[i], 10)) ;
    }
    return threadinfos;
  }
 
  public long getThreadCpuTime(long tid){ return this.mbean.getThreadCpuTime(tid); }
  public long getThreadUserTime(long tid){ return this.mbean.getThreadUserTime(tid);}
  
  public String getThreadStartedCount() { return threadStartedCount; }
  public void setThreadStartedCount(String threadStartedCount) { this.threadStartedCount = threadStartedCount; }
  public String getThreadPeakCount() { return threadPeakCount;}
  public void setThreadPeakCount(String threadPeakCount) { this.threadPeakCount = threadPeakCount;}

  public String getThreadCount() { return threadCount; }
  public void setThreadCount(String threadCount) { this.threadCount = threadCount;}
  
  public String getThreadDeamonCount() { return threadDeamonCount; }
  public void setThreadDeamonCount(String threadDeamonCount) { this.threadDeamonCount = threadDeamonCount; }
  
  public String getPrintStackTrace(StackTraceElement[] elements) {
    String result = "";
    for(StackTraceElement element : elements) {
      result += "      " + element.toString() + "\n" ;
    }
    return result;
  }

}
