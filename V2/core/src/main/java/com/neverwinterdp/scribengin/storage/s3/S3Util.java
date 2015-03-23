package com.neverwinterdp.scribengin.storage.s3;


public class S3Util {
  static public int getStreamId(String name) {
    int dashIdx = name.lastIndexOf('-');
    return Integer.parseInt(name.substring(dashIdx + 1)) ;
  }
}
