package com.neverwinterdp.scribengin.dependency;

public interface Server {

  String getHost();

  int getPort();

  void start() throws Exception;

  void shutdown();

}
