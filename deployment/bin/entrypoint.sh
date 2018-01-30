#!/bin/bash

# If no arguments or "server" start the search API
if [ $# -eq 0 ] || [ "${1}" = "server" ]; then
  echo "Starting jetty container for search API"
  bash /kb/deployment/bin/start_server.sh
elif [ "${1}" = "coordinator" ] ; then
  echo "Starting search coordinator process"
  /kb/deployment/bin/search_tools.sh -c /kb/deployment/conf/search_tools.cfg -s
elif [ "${1}" = "worker" ] ; then
  echo "Starting search worker process"
  /kb/deployment/bin/search_tools.sh -c /kb/deployment/conf/search_tools.cfg -k $HOSTNAME
else
  echo Unknown
fi
