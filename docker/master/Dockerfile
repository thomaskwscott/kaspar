FROM confluentinc/cp-server:5.5.0

# install spark
COPY resources/spark-2.4.5-bin-hadoop2.7 /usr/local/spark
ENV SPARK_HOME=/usr/local/spark

#copy run script
COPY resources/launchMaster.sh /home/ubuntu/launchMaster.sh

CMD ["/home/ubuntu/launchMaster.sh"]