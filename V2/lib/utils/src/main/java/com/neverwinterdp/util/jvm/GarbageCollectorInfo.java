package com.neverwinterdp.util.jvm;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;

import com.neverwinterdp.util.text.NumberFormatter;
import com.neverwinterdp.util.text.StringUtil;

public class GarbageCollectorInfo implements Serializable{
  private String name;
  private String collectionCount;
  private String collectionTime;
  private String poolNames;

  public GarbageCollectorInfo(GarbageCollectorMXBean gcbean) {
    name = gcbean.getName();
    collectionCount = Long.toString(gcbean.getCollectionCount());
    collectionTime = NumberFormatter.milliTimeAsHumanReadable(gcbean.getCollectionTime());
    poolNames = StringUtil.joinStringArray(gcbean.getMemoryPoolNames(), "|");
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name;}

  public String getCollectionCount() { return collectionCount; }
  public void setCollectionCount(String collectionCount) { this.collectionCount = collectionCount; }

  public String getCollectionTime() { return collectionTime; }
  public void setCollectionTime(String collectionTime) { this.collectionTime = collectionTime;}

  public String getPoolNames() { return poolNames; }
  public void setPoolNames(String poolNames) { this.poolNames = poolNames; }
}
