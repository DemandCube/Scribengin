package com.neverwinterdp.registry.election;

import com.neverwinterdp.util.text.StringUtil;


public class LeaderId implements Comparable<LeaderId> {
  private String  path ;
  private long    sequence ;
  
  public LeaderId(String path) {
    this.path = path ;
    int idx = path.lastIndexOf('/') ;
    String nodeName = path.substring(idx + 1) ;
    String[] parts = StringUtil.toStringArray(nodeName, "-") ;
    this.sequence = Long.parseLong(parts[1]) ;
  }
  
  public String getPath() { return path; }

  public long getSequence() { return sequence; }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if(o == null || getClass() != o.getClass())
      return false;

    LeaderId otherId = (LeaderId) o;
    return path.equals(otherId.path) ;
  }
  
  @Override
  public int hashCode() { return path.hashCode() + 37; }

  public int compareTo(LeaderId that) {
    long s1 = this.sequence;
    long s2 = that.sequence;
    long ret = s1 == -1 ? 1 : s2 == -1 ? -1 : s1 - s2;
    if(ret < 0) return -1 ;
    else if(ret > 0) return 1 ;
    return 0 ;
  }

  public String toString() { return this.path ; }
}
