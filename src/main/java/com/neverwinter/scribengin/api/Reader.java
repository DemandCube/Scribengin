package com.neverwinter.scribengin.api;

import org.apache.commons.chain.Command;

// Read input from e.g kafka
public interface Reader<T> extends Command {

  T read();
}
