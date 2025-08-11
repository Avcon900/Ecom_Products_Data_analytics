FROM spark:3.5.0-scala2.12-java11-python3-ubuntu
USER root
WORKDIR /opt/spark/work-dir

ARG WORKDIR=/opt/spark/work-dir
ENV DELTA_PACKAGE_VERSION=delta-spark_3.1.0:${DELTA_SPARK_VERSION}
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY requirements-docker.txt ${WORKDIR}/requirements-docker.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r ${WORKDIR}/requirements-docker.txt \
    && rm -f ${WORKDIR}/requirements-docker.txt

RUN apt -qq update
RUN apt -qq -y install vim curl

RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    && wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
    && wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar 

COPY hive-site.xml /opt/spark/conf/hive-site.xml

COPY . ${WORKDIR}