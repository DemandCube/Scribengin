package com.neverwinterdp.scribengin;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import com.google.gson.Gson;

public class ScribeLogEntry {
  private long startOffset;
  private long endOffset;
  private String srcPath;
  private String destPath;
  private byte[] checksum;

  public ScribeLogEntry(long startOffset, long endOffset, String srcPath, String destPath) throws NoSuchAlgorithmException {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.srcPath = srcPath;
    this.destPath = destPath;
    this.generateCheckSum();
  }

  private byte[] calcCheckSum() throws NoSuchAlgorithmException {
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

  public static ScribeLogEntry fromJson(String jsonStr) throws NoSuchAlgorithmException {
    ScribeLogEntry entry = (new Gson()).fromJson(jsonStr, ScribeLogEntry.class);
    if (Arrays.equals(entry.calcCheckSum(), entry.checksum)) {
      return entry;
    }
    return null;
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

