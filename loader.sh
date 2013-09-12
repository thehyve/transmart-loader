#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -cp `find $DIR/lib -name 'weld-logger-*.jar'`:`find $DIR -name 'loader-*.jar'` \
	com.recomdata.pipeline.entrypoint.Bootstrap $@
