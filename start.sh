#!/usr/bin/bash

if [ -z "$(pgrep -f './server')" ]; then
	./server > /dev/null 2>&1 &
	echo $! > server.pid
	sleep 0.1
fi

if [ -z "$(pgrep -f 'serve -s build/')" ]; then
	serve -s build/ &
	sleep 0.1
	echo $(pgrep -f "serve -s build/") > serve.pid
	
fi

google-chrome http://localhost:3000 > /dev/null 2>&1
