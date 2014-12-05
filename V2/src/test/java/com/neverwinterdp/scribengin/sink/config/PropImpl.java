package com.neverwinterdp.scribengin.sink.config;

import java.lang.annotation.Annotation;

public class PropImpl implements Prop {

  private final Property value;

  public PropImpl(Property value) {
      this.value = value;
  }

  @Override
  public Property value() {
      return value;
  }

  @Override
  public int hashCode() {
      // This is specified in java.lang.Annotation.
      return (127 * "value".hashCode()) ^ value.hashCode();
  }

  @Override
  public boolean equals(Object o) {
      if (!(o instanceof Prop)) {
          return false;
      }

      Prop other = (Prop) o;
      return value.equals(other.value());
  }

  @Override
  public String toString() {
      return "@" + Prop.class.getName() + "(value=" + value + ")";
  }

  @Override
  public Class<? extends Annotation> annotationType() {
      return Prop.class;
  }
}