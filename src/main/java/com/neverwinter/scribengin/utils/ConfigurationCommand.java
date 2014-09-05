package com.neverwinter.scribengin.utils;

import java.io.Serializable;

public class ConfigurationCommand implements Serializable {


  private static final long serialVersionUID = 1L;
  private String memberName;
  private String timestamp;
  private boolean executed;

  public String getMemberName() {
    return memberName;
  }

  public void setMemberName(String memberName) {
    this.memberName = memberName;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public boolean isObeyed() {
    return executed;
  }

  public void setObeyed(boolean obeyed) {
    this.executed = obeyed;
  }
}
