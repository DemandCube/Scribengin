package com.neverwinterdp.dependency;

public interface Server {

  String getHost();

  int getPort();

  void start() throws Exception;

  void shutdown();

}
