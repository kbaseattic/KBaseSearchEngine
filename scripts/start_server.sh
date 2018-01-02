#!/bin/bash
script_dir=$(dirname "$(readlink -f "$0")")
cd scripts
java -Xmx1g -cp /Users/sychan/src/jars/lib/jars/jetty/jetty-start-7.0.0.jar:/Users/sychan/src/jars/lib/jars/jetty/jetty-all-7.0.0.jar:/Users/sychan/src/jars/lib/jars/servlet/servlet-api-2.5.jar 	-DKB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg -Djetty.port=5000 org.eclipse.jetty.start.Main jetty.xml
