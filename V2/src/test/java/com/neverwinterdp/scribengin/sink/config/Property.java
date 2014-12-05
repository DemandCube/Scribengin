package com.neverwinterdp.scribengin.sink.config;

import com.google.inject.TypeLiteral;

public enum Property {
  FOO(
      "foo.server",
      "Defines the foo server",
      new TypeLiteral<String>() {},
      "DefaultFooSerevr")
  ;

  private String name;
  private String description;
  private TypeLiteral<?> type;
  private String defaultValue;

  private <V> Property(String name, String description,
      TypeLiteral<V> type, String defaultValue) {
    this.name = name;
    this.description = description;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  @SuppressWarnings("unchecked")
  public <V> TypeLiteral<V> getValueType() {
      return (TypeLiteral<V>) type;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

}
