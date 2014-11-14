package com.neverwinterdp.scribengin.registry.lock;

import com.neverwinterdp.util.text.StringUtil;


public class LockId implements Comparable<LockId> {
  private String  path ;
  private String  nodeName ;
  private String  name   ;
  private String  session  ;
  private long    sequence ;
  
  public LockId(String path) {
    this.path = path ;
    int idx = path.lastIndexOf('/') ;
    nodeName = path.substring(idx + 1) ;
    String[] parts = StringUtil.toStringArray(nodeName, "-") ;
    this.name = parts[0];
    this.session = parts[1] ;
    this.sequence = Long.parseLong(parts[2]) ;
  }
  
  public String getPath() { return path; }

  public String getNodeName() { return this.nodeName; }
  
  public String getName() { return name; }

  public String getSession() { return session; }

  public long getSequence() { return sequence; }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if(o == null || getClass() != o.getClass())
      return false;

    LockId otherId = (LockId) o;
    return path.equals(otherId.path) ;
  }
  
  @Override
  public int hashCode() {
    return path.hashCode() + 37;
  }

  public int compareTo(LockId that) {
    int answer = this.name.compareTo(that.name);
    if (answer == 0) {
      long s1 = this.sequence;
      long s2 = that.sequence;
      long ret = s1 == -1 ? 1 : s2 == -1 ? -1 : s1 - s2;
      if(ret < 0) return -1 ;
      else if(ret > 0) return 1 ;
      return 0 ;
    }
    return answer;
  }

  public String toString() { return this.path ; }
}
