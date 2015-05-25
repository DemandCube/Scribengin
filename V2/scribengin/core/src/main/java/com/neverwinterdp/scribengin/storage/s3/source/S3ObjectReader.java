package com.neverwinterdp.scribengin.storage.s3.source;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.regex.Pattern;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.util.JSONSerializer;

public class S3ObjectReader implements Closeable {

  protected static final Pattern PATTERN = Pattern.compile("(?=<)|(?<=})");
  private InputStream inputStream;
  private Scanner streamReader;

  public S3ObjectReader(InputStream inputStream) {
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    streamReader = new Scanner(bis, StandardCharsets.UTF_8.name());
    streamReader.useDelimiter(PATTERN);
    this.inputStream = inputStream;
  }

  public Record next() {
    return JSONSerializer.INSTANCE.fromString(streamReader.next(), Record.class);
  }

  public boolean hasNext() {
    return streamReader.hasNext();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
