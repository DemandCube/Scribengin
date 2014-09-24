package com.neverwinterdp.scribengin.hostport;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IStringConverterFactory;

public class CustomConvertFactory implements IStringConverterFactory {
  public Class<? extends IStringConverter<?>> getConverter(Class forType) {
    if (forType.equals(HostPort.class)) {
      return HostPortConverter.class;
    } else
      return null;
  }

}
