FROM ubuntu
RUN apt-get update && apt upgrade -y
RUN apt-get install curl mlocate default-jdk -y
RUN apt-get install wget nano -y
RUN wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
RUN tar xvf spark-3.4.0-bin-hadoop3.tgz
RUN mv spark-3.4.0-bin-hadoop3/ /opt/spark/
RUN cd /home/
RUN touch /home/.bashrc
RUN echo "SPARK_HOME=/opt/spark" >> /home/.bashrc
RUN echo "PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> /home/.bashrc
EXPOSE 80