package com.neverwinterdp.scribengin.hostport;

import com.beust.jcommander.IStringConverter;

public class HostPortConverter implements IStringConverter<HostPort> {

  public HostPort convert(String value) {
    String[] s = value.split(":");
    HostPort result = new HostPort(s[0], s[1]);

    return result;
  }

}
