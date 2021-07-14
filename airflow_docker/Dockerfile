# REVERSE-ENGR: BRIGHT EMAH
# DESCRIPTION: Airflow container with JAVA 8 and SPARK binaries
# BASED ON: https://github.com/puckel/docker-airflow

FROM puckel/docker-airflow:latest

USER root
RUN whoami
RUN apt-get update
RUN apt-get install sudo

RUN apt-get update \
 && apt-get install -y locales \
 && dpkg-reconfigure -f noninteractive locales \
 && locale-gen C.UTF-8 \
 && /usr/sbin/update-locale LANG=C.UTF-8 \
 && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
 && locale-gen \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Users with other locales should set this in their derivative image
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list                                                   

RUN mkdir -p /usr/share/man/man1

# JAVA
RUN apt-get update \
 && apt-get install -y openjdk-8-jre \ 
 && apt-get install unzip -y \
 && apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
 
# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get update \
 && apt-get install -y vim \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
 
RUN apt-get update \
 && apt-get install -y dos2unix \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* 

# Install Hadoop
ENV HADOOP_VERSION=2.7.3
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/conf
ENV PATH $PATH:$HADOOP_HOME/bin
RUN mkdir -p $HADOOP_HOME
RUN curl -sL \
  "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
    | gunzip \
    | tar -x -C /opt/ \
  && rm -rf $HADOOP_HOME/share/doc \
  && chown -R root:root $HADOOP_HOME \
  && mkdir -p $HADOOP_HOME/logs \
  && mkdir -p $HADOOP_CONF_DIR \
  && chmod 777 $HADOOP_CONF_DIR \
  && chmod 777 $HADOOP_HOME/logs 


# Install Spark
ENV SPARK_VERSION=2.3.1
ENV SPARK_HOME /opt/spark-$SPARK_VERSION-bin-hadoop2.7
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV PATH $PATH:$SPARK_HOME/bin
RUN mkdir -p $SPARK_HOME
RUN curl -sL \
  "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz" \
    | gunzip \
    | tar -x -C /opt/ \
  && chown -R root:root $SPARK_HOME \
  && mkdir -p /data/spark/ \
  && mkdir -p $SPARK_HOME/logs \
  && mkdir -p $SPARK_CONF_DIR \
  && chmod 777 $SPARK_HOME/logs


USER $USERNAME
RUN whoami
EXPOSE 22
EXPOSE 4040
EXPOSE 10000

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]

WORKDIR /usr/local/airflow/
COPY requirements.txt .
RUN pip3 install --user -r requirements.txt