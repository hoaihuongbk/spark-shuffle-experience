FROM apache/spark:3.5.0

USER root
# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

COPY extra-jars/* $SPARK_HOME/jars/
RUN chown spark:spark $SPARK_HOME/jars/*

WORKDIR /app
USER spark

