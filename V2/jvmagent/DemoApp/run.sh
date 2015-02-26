#!/bin/bash

java \
  -javaagent:../registry/build/release/jvmagent.bootstrap-1.0-SNAPSHOT.jar="../registry/build/release" \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=10001 \
  -Dcom.sun.management.jmxremote.local.only=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.local.only=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -jar build/libs/jvmagent.DemoApp-1.0-SNAPSHOT.jar
