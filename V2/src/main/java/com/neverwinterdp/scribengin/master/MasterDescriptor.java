package com.neverwinterdp.scribengin.master;

import java.util.Comparator;


public class MasterDescriptor {
  static public enum Type { LEADER, FOLLOWER }
  
  private String id ;
  private Type   type = Type.FOLLOWER;

  public String getId() { return id;}
  public void setId(String id) { this.id = id;}
  
  public Type getType() { return type; }
  public void setType(Type type) { this.type = type; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("id = ").append(id).append(", ");
    b.append("type = ").append(type);
    return b.toString();
  }
  
  static public Comparator<MasterDescriptor> COMPARATOR = new Comparator<MasterDescriptor>() {
    public int compare(MasterDescriptor o1, MasterDescriptor o2) {
      String id1 = o1.getId(); 
      String id2 = o2.getId();
      if(id1 == null && id2 == null) return 0;
      if(id1 != null && id2 == null) return -1;
      if(id1 == null && id2 != null) return  1;
      return id1.compareTo(id2);
    }
  };
}
