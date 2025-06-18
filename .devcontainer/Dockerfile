FROM mcr.microsoft.com/devcontainers/python:3.10

# Java and Spark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.0-bin-hadoop3.tgz && \
    mv spark-3.3.0-bin-hadoop3 /opt/spark && \
    rm spark-3.3.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
