FROM bde2020/spark-base:2.4.5-hadoop2.7
COPY target/spark-tp2-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "batch.Ex2", "--master", "spark://spark-master:7077", "/spark-tp2-1.0-SNAPSHOT.jar"]