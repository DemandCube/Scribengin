package com.neverwinterdp.scribengin;

import java.security.MessageDigest;

import org.apache.commons.net.util.Base64;

public class RecordChecksum {
  private MessageDigest keyDigest ;
  private MessageDigest dataDigest ;
  private boolean invalidUpdateState ;
  
  public RecordChecksum() throws Exception {
    keyDigest =  MessageDigest.getInstance("sha1");
    dataDigest =  MessageDigest.getInstance("sha1");
  }
  
  synchronized public void update(Record record) throws Exception {
    if(invalidUpdateState) {
      throw new Exception("Invalid state, cannot add update record") ;
    }
    keyDigest.update(record.getKey().getBytes());
    dataDigest.update(record.getData());
  }
  
  public byte[] getKeyDigest() {
    invalidUpdateState = true;
    return keyDigest.digest(); 
  }
  
  public String getKeyDigestAsBase64() { 
    invalidUpdateState = true;
    return Base64.encodeBase64String(keyDigest.digest()); 
  }
  
  public byte[] getDataDigest() {
    invalidUpdateState = true;
    return dataDigest.digest(); 
  }
  
  public String getDataDigestAsBase64() {
    invalidUpdateState = true;
    return Base64.encodeBase64String(dataDigest.digest()); 
  }
}