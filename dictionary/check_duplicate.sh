#!/usr/bin/env bash
PROJECT_DIR=$(cd $(dirname $0)/..; pwd)
cat ${PROJECT_DIR}/dictionary/stopwords.csv | sort | uniq -c | awk '{print $1 " " $2}' | grep -v 1
