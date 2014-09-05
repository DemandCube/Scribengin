package com.neverwinter.scribengin;

import java.io.Serializable;


/*
 * Cluster members report to zk their progress
 * 
 * Who am I
 * What have I done?
 * When did i do it?
 * Where did I get the data from?
 */
public class Progress implements Serializable {

  private static final long serialVersionUID = 2714669206543561261L;
  private String memberName;
  private int offset;
  private String kafkaBroker;
  private String timestamp;

  public String getMemberName() {
    return memberName;
  }

  public void setMemberName(String memberName) {
    this.memberName = memberName;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public String getKafkaBroker() {
    return kafkaBroker;
  }

  public void setKafkaBroker(String kafkaBroker) {
    this.kafkaBroker = kafkaBroker;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

}
