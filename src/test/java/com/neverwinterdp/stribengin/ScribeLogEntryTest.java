package com.neverwinterdp.stribengin;

import java.security.NoSuchAlgorithmException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.neverwinterdp.scribengin.ScribeLogEntry;

public class ScribeLogEntryTest extends TestCase {

  public ScribeLogEntryTest(String name)
  {
    super(name); }

  public static Test suite()
  {
    return new TestSuite( ScribeLogEntryTest.class );
  }

  public void testToJson()
  {
    String expectedStr = "{\"startOffset\":0,\"endOffset\":10,\"srcPath\":\"/src/path/data.123\",\"destPath\":\"/dst/path/data.123\",\"checksum\":[-128,55,-36,-117,-44,-98,-114,-54,39,-104,-40,-25,-33,86,-63,-47]}";
    try {
      ScribeLogEntry entry = new ScribeLogEntry(0, 10, "/src/path/data.123", "/dst/path/data.123");
      String s = ScribeLogEntry.toJson(entry);
      assertTrue(expectedStr.equals(s));
    } catch (NoSuchAlgorithmException e) {
      assert(false); // no md5
    }
  }

  public void testFromJson()
  {
    String jsonStr = "{\"startOffset\":0,\"endOffset\":10,\"srcPath\":\"/src/path/data.123\",\"destPath\":\"/dst/path/data.123\",\"checksum\":[-128,55,-36,-117,-44,-98,-114,-54,39,-104,-40,-25,-33,86,-63,-47]}";
    ScribeLogEntry entry = ScribeLogEntry.fromJson(jsonStr);
    assertTrue( entry.getStartOffset()==0 );
    assertTrue( entry.getEndOffset()==10 );
    assertTrue( entry.getSrcPath().equals("/src/path/data.123") );
    assertTrue( entry.getDestPath().equals("/dst/path/data.123") );
    try {
      assertTrue( entry.isCheckSumValid() );
    } catch (NoSuchAlgorithmException e) {
      assert(false); // no md5
    }
  }

  public void testFromJsonWithBadChecksum()
  {
    String jsonStr = "{\"startOffset\":0,\"endOffset\":10,\"srcPath\":\"/src/path/data.123\",\"destPath\":\"/dst/path/data.123\",\"checksum\":[-128,55,-36,-117,-44,-98,-114,-54,39,-104,-40,-25,-33,86,-63,-48]}";
    try {
      ScribeLogEntry entry = ScribeLogEntry.fromJson(jsonStr);
      //assert(entry == null);
      assert(entry.isCheckSumValid() == false);
    } catch (NoSuchAlgorithmException e) {
      assert(false); // no md5
    }
  }
}
