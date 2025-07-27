FROM ubuntu:22.04 AS base

ARG spark_version="3.4.1"
ARG delta_version="2.4.0"
ARG openjdk_version="17"
ARG python_version="3.10"


RUN apt update && apt install -y openjdk-${openjdk_version}-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install Spark
RUN \
    apt update && \
    apt install -y wget && \
    wget https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop3.tgz && \
    tar xvf spark-${spark_version}-bin-hadoop3.tgz && \
    mv spark-${spark_version}-bin-hadoop3/ /opt/spark && \
    export SPARK_HOME=/opt/spark && \
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && \
    export PYSPARK_PYTHON=/usr/bin/python3 && \
    rm spark-${spark_version}-bin-hadoop3.tgz

# Install Python in new layer to improve rebuilds with different versions
RUN \
    apt update && \
    apt install -y python${python_version} python3-pip && \
    rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Copy application files AFTER installing Spark and Hadoop
COPY . /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-docker.txt
