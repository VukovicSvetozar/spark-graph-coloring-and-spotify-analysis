#!/bin/bash

# Skripta za pokretanje Graph Coloring App

# Java opcije za Spark (rješava probleme modula)
JAVA_OPTS="
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/sun.security.action=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
"

CP="target/classes:$(cat cp.txt)"

MAIN_CLASS="org.etf.graph.MainApp"

java $JAVA_OPTS -cp "$CP" $MAIN_CLASS "$@"