FROM kbase/kbase:sdkbase.latest
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# RUN apt-get update

# update jars
RUN cd /kb/dev_container/modules/jars \
	&& git pull \
	&& . /kb/dev_container/user-env.sh \
	&& make deploy \
	&& echo docker is annoying 1

RUN apt-get install nano \
	&& add-apt-repository ppa:openjdk-r/ppa \
	&& sudo apt-get update \
	&& sudo apt-get -y install openjdk-8-jdk \
	&& echo java versions: \
	&& java -version \
	&& javac -version \
	&& echo $JAVA_HOME \
	&& ls -l /usr/lib/jvm \
	&& cd /kb/runtime \
	&& rm java \
	&& ln -s /usr/lib/jvm/java-8-openjdk-amd64 java \
	&& ls -l

# Need to think about how to get tests to run in TravisCI with different versions
RUN cd /opt \
	&& wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.0.tar.gz \
	&& tar xfz elasticsearch-5.5.0.tar.gz \
	&& ln -s elasticsearch-5.5.0 elasticsearch
	
	
RUN cd /opt \
	&& wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.6.12.tgz \
	&& tar xfz mongodb-linux-x86_64-2.6.12.tgz \
	&& ln -s mongodb-linux-x86_64-2.6.12 mongo

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work
RUN chmod -R a+rw /kb/module

WORKDIR /kb/module

RUN make all

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
