package com.neverwinterdp.scribengin.storage.s3;

import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Pattern;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.util.JSONSerializer;

public class ScannerStuff {
  
  protected static final Pattern PARAMETER_PATTERN =
      Pattern.compile("(?=<)|(?<=})");
  public static void main(String[] args) {
    
    String data = "{\"key\":\"stream=1,buffer=0,record=0\",\"data\":\"c3RyZWFtPTEsYnVmZmVyPTAscmVjb3JkPTA=\"}{\"key\":\"stream=1,buffer=1,record=1\",\"data\":\"c3RyZWFtPTEsYnVmZmVyPTEscmVjb3JkPTE=\"}";
    Scanner scanner = new Scanner(data).useDelimiter(PARAMETER_PATTERN);
    String record;
    
  String[] datas=  data.split("(?=<)|(?<=})");
  System.out.println(data);
  System.out.println(datas.length);
  for (String string : datas) {
    System.out.println("string "+string);
  }
    while (scanner.hasNext()) {
      record = scanner.next();
      Record x = JSONSerializer.INSTANCE.fromString(record, Record.class);
      System.out.println("record " + x.getKey() + " data " + Arrays.toString(x.getData()));
    }

  }
}
