package com.neverwinterdp.scribengin.writers;

import java.io.IOException;

import org.apache.commons.chain.Command;

public interface Writer<T> extends Command {

  void write(T data) throws IOException;
}
