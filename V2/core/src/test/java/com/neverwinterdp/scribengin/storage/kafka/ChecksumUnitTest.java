package com.neverwinterdp.scribengin.storage.kafka;

import java.security.MessageDigest;

import org.apache.commons.net.util.Base64;
import org.junit.Test;

public class ChecksumUnitTest {
  @Test
  public void testMD5Checksum() throws Exception { 
    MessageDigest md5 =  MessageDigest.getInstance("sha1");
    for(int i = 0; i < 10; i++) {
      String key = "key - " + i;
      md5.update(key.getBytes());
    }
    System.out.println("MD5 fingerprint: " + Base64.encodeBase64String(md5.digest()));
    System.out.println("-------------------------------------");
    for(int i = 9 ; i <= 0 ; i--) {
      String key = "key - " + i;
      byte[] bytes = key.getBytes();
      md5.update(bytes, 0, bytes.length);
    }
    System.out.println("MD5 fingerprint: " + Base64.encodeBase64String(md5.digest()));
  }
}