FROM ubuntu:22.04

# Install Java, Python, pip, and basic tools
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk python3 python3-pip wget curl nano unzip && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Install Spark
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3

RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
