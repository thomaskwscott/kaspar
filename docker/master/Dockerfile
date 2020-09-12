FROM confluentinc/cp-server:5.5.1

# install spark
COPY resources/spark-3.0.0-bin-hadoop2.7 /usr/local/spark
ENV SPARK_HOME=/usr/local/spark

#copy run script
COPY resources/launchMaster.sh /home/ubuntu/launchMaster.sh

# install java 11
RUN apt-get update
RUN apt-get remove -y zulu-8
RUN apt-get install -y zulu-11

CMD ["/home/ubuntu/launchMaster.sh"]