#!/bin/sh -x
# Set default memory sizes that can be overridden by environment vars
MIN_MEM=${max_mem:-"1000"}
MAX_MEM=${min_mem:-"3000"}

# Setup the JVM options using the JDK 1.7+ environment var _JAVA_OPTIONS
export _JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${MAX_MEM}m -XX:+UseG1GC"

cd /kb/deployment/services/search/tomcat/webapps/
unzip ROOT.war
java -cp "WEB-INF/lib/*"  kbasesearchengine.tools.SearchTools $@
    