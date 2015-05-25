package com.neverwinterdp.util.jvm;

import java.io.Serializable;
import java.lang.management.ThreadInfo;

public class AppThreadInfo implements Serializable{
  private String threadName;
  private String threadId;
  private String threadBlockCount;
  private String threadBlockTime;
  private String threadWaitedCount;
  private String threadWaitedTime;
  private String threadState;
  private String threadCPUTime;
  private String threadUserTime;
  private String threadStackTrace;
  
  public AppThreadInfo() {} 
  
  public AppThreadInfo(ThreadInfo tinfo) {
    threadName = tinfo.getThreadName();
    threadId = Long.toString(tinfo.getThreadId());
    threadBlockCount = Long.toString(tinfo.getBlockedCount());
    threadBlockTime = Long.toString(tinfo.getBlockedTime());
    threadWaitedCount = Long.toString(tinfo.getWaitedCount());
    threadWaitedTime = Long.toString(tinfo.getWaitedTime());
    threadState = tinfo.getThreadState().toString();
    threadStackTrace = getPrintStackTrace(tinfo.getStackTrace());
  }
  public String getPrintStackTrace(StackTraceElement[] elements) {
    String result = "";
    for(StackTraceElement element : elements) {
      result += "      " + element.toString() + "\n" ;
    }
    return result;
  }
  public String getThreadName() { return threadName; }
  public void setThreadName(String threadName) { this.threadName = threadName; }
  
  public String getThreadId() { return threadId; }
  public void setThreadId(String threadId) { this.threadId = threadId; }
  
  public String getThreadBlockCount() { return threadBlockCount; }
  public void setThreadBlockCount(String threadBlockCount) { this.threadBlockCount = threadBlockCount;}
 
  public String getThreadBlockTime() { return threadBlockTime; }
  public void setThreadBlockTime(String threadBlockTime) { this.threadBlockTime = threadBlockTime; }
  
  public String getThreadWaitedCount() {return threadWaitedCount;}
  public void setThreadWaitedCount(String threadWaitedCount) {this.threadWaitedCount = threadWaitedCount; }
 
  public String getThreadWaitedTime() { return threadWaitedTime; }
  public void setThreadWaitedTime(String threadWaitedTime) { this.threadWaitedTime = threadWaitedTime; }
 
  public String getThreadState() { return threadState; }
  public void setThreadState(String threadState) { this.threadState = threadState; }

  public String getThreadCPUTime() { return threadCPUTime; }
  public void setThreadCPUTime(String threadCPUTime) { this.threadCPUTime = threadCPUTime; }

  public String getThreadUserTime() { return threadUserTime; }
  public void setThreadUserTime(String threadUserTime) { this.threadUserTime = threadUserTime; }
  
  public String getThreadStackTrace() { return threadStackTrace; }
  public void setThreadStackTrace(String threadStackTrace) { this.threadStackTrace = threadStackTrace;}
}
