package com.neverwinterdp.scribengin;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class ScribeLogEntry {
  private long startOffset;
  private long endOffset;
  private String srcPath;
  private String destPath;
  private byte[] checksum;

  public ScribeLogEntry() {
  }

  public ScribeLogEntry(long startOffset, long endOffset, String srcPath, String destPath) throws NoSuchAlgorithmException {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.srcPath = srcPath;
    this.destPath = destPath;
    this.generateCheckSum();
  }

  // Return null if the private fields aren't properly instantiated.
  private byte[] calcCheckSum() throws NoSuchAlgorithmException {
    if ( srcPath == null || destPath == null)
      return null;
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(ByteBuffer.allocate(8).putLong(startOffset).array()); //startOffset
    md.update(ByteBuffer.allocate(8).putLong(endOffset).array()); //endOffset
    md.update(srcPath.getBytes()); // srcPath
    md.update(destPath.getBytes()); // destPath
    return md.digest();
  }

  private void generateCheckSum() throws NoSuchAlgorithmException {
    this.checksum = calcCheckSum();
  }

  public static String toJson(ScribeLogEntry entry) throws NoSuchAlgorithmException {
    return (new Gson()).toJson(entry);
  }

  public static ScribeLogEntry fromJson(String jsonStr) {
    ScribeLogEntry r;
    try {
      r = (new Gson()).fromJson(jsonStr, ScribeLogEntry.class);
    } catch(JsonSyntaxException e) {
      r = new ScribeLogEntry();
    }
    return r;
  }

  public boolean isCheckSumValid() throws NoSuchAlgorithmException{
    byte[] calculatedChecksum = calcCheckSum();
    if (calculatedChecksum == null) {
      return false;
    } else {
      return Arrays.equals(calculatedChecksum, checksum);
    }
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getEndOffset() {
    return endOffset;
  }

  public String getSrcPath() {
    return srcPath;
  }

  public String getDestPath() {
    return destPath;
  }

}

