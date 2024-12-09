#!/bin/bash

# Get the directory where the script is located
# Get the directory where the script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Define our classpath
CLASSPATH="$DIR/crawler.jar:$DIR/lib/flame.jar:$DIR/lib/kvs.jar:$DIR/lib/webserver.jar"

# Print classpath for debugging
echo "Using classpath: $CLASSPATH"

# Check if any arguments were provided
if [ $# -eq 0 ]; then
    echo "Error: No seed URLs provided"
    echo "Usage: $0 <url1> [url2] [url3] ..."
    exit 1
fi

# Print all seed URLs
echo "Submitting job to crawl the following URLs:"
for url in "$@"; do
    echo "- $url"
done

# Join all URLs with commas to pass as a single argument
SEED_URLS=$(IFS=,; echo "$*")

# Run the crawler with all seed URLs
java -cp "$CLASSPATH" cis5550.flame.FlameSubmit localhost:9000 "$DIR/crawler.jar" cis5550.jobs.Crawler "$SEED_URLS"
