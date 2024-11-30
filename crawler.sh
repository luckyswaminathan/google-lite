#!/bin/bash

# Get the directory where the script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Define our classpath
CLASSPATH="$DIR/crawler.jar:$DIR/lib/flame.jar:$DIR/lib/kvs.jar:$DIR/lib/webserver.jar"

# Run with debug output to see what's happening
echo "Using classpath: $CLASSPATH"
echo "Submitting job to crawl: $1"

java -cp "$CLASSPATH" cis5550.flame.FlameSubmit localhost:9000 "$DIR/crawler.jar" cis5550.jobs.Crawler "$1"
