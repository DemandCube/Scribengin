package com.neverwinterdp.scribengin;

import java.security.NoSuchAlgorithmException;

import com.neverwinterdp.scribengin.ScribeLogEntry;
import org.junit.Assert;
//import org.junit.Ignore;
import org.junit.Test;

public class ScribeLogEntryTest {

  //@Ignore
  @Test
  public void testToJson()
  {
    String expectedStr = "{\"startOffset\":0,\"endOffset\":10,\"srcPath\":\"/src/path/data.123\",\"destPath\":\"/dst/path/data.123\",\"checksum\":[-128,55,-36,-117,-44,-98,-114,-54,39,-104,-40,-25,-33,86,-63,-47]}";
    try {
      ScribeLogEntry entry = new ScribeLogEntry(0, 10, "/src/path/data.123", "/dst/path/data.123");
      String s = ScribeLogEntry.toJson(entry);
      Assert.assertTrue(expectedStr.equals(s));
    } catch (NoSuchAlgorithmException e) {
      assert(false); // no md5
    }
  }

  //@Ignore
  @Test
  public void testFromJson()
  {
    String jsonStr = "{\"startOffset\":0,\"endOffset\":10,\"srcPath\":\"/src/path/data.123\",\"destPath\":\"/dst/path/data.123\",\"checksum\":[-128,55,-36,-117,-44,-98,-114,-54,39,-104,-40,-25,-33,86,-63,-47]}";
    ScribeLogEntry entry = ScribeLogEntry.fromJson(jsonStr);
    Assert.assertTrue( entry.getStartOffset()==0 );
    Assert.assertTrue( entry.getEndOffset()==10 );
    Assert.assertTrue( entry.getSrcPath().equals("/src/path/data.123") );
    Assert.assertTrue( entry.getDestPath().equals("/dst/path/data.123") );
    try {
      Assert.assertTrue( entry.isCheckSumValid() );
    } catch (NoSuchAlgorithmException e) {
      assert(false); // no md5
    }
  }

  //@Ignore
  @Test
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
