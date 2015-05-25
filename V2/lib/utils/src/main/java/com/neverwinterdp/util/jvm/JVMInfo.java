package com.neverwinterdp.util.jvm;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.NumberFormatter;


public class JVMInfo implements Serializable {
  private String startTime ;
  private String upTime ;
  
  private MemoryInfo memoryInfo;
  private ArrayList<GarbageCollectorInfo> garbageCollectorInfo ;
  private AppInfo appInfo;
  
  public JVMInfo() {
    this.startTime = DateUtil.asCompactDateTime(ManagementFactory.getRuntimeMXBean().getStartTime()) ;
    this.upTime = NumberFormatter.milliTimeAsHumanReadable(ManagementFactory.getRuntimeMXBean().getUptime()) ;
    memoryInfo = new MemoryInfo();
    
    List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans() ; 
    garbageCollectorInfo = new ArrayList<GarbageCollectorInfo>();
    for(int i = 0; i < gcbeans.size(); i++) {
      GarbageCollectorMXBean gcbean = gcbeans.get(i) ;
      garbageCollectorInfo.add(new GarbageCollectorInfo(gcbean));
    }
    
    appInfo = new AppInfo();
  }
  
  public String getStartTime() { return this.startTime ; }
  
  public String getUpTime() { return this.upTime ; }
  
  public MemoryInfo getMemoryInfo() { return memoryInfo;}

  public ArrayList<GarbageCollectorInfo> getGarbageCollectorInfo() { 
    return garbageCollectorInfo; 
  }

  public AppInfo getThreadInfo() { return appInfo; }
}