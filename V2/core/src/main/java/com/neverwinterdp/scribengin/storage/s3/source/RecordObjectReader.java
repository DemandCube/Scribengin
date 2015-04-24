package com.neverwinterdp.scribengin.storage.s3.source;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.regex.Pattern;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.util.JSONSerializer;

public class RecordObjectReader implements Closeable {

  protected static final Pattern PATTERN = Pattern.compile("(?=<)|(?<=})");
  private InputStream inputStream;
  private Scanner scanner;

  public RecordObjectReader(InputStream inputStream) {
    super();
    this.scanner = new Scanner(inputStream);
    scanner.useDelimiter(PATTERN);
    this.inputStream = inputStream;
  }

  public Record next() {
    return JSONSerializer.INSTANCE.fromString(scanner.next(), Record.class);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  public boolean hasNext() {
    return scanner.hasNext();
  }
}
