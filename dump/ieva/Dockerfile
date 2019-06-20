FROM mjhea0/spark:2.4.1
RUN apt-get update
RUN apt-get install -y wget
RUN mkdir /mongo
USER root
RUN wget "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/3527bf39-d93e-4071-8451-df2ade1ea4f2/Power-Networks-LCL-June2015.zip" -P  /usr/spark-2.4.1/volume/
RUN unzip /usr/spark-2.4.1/volume/Power-Networks-LCL-June2015.zip 
#VOLUME mongo:/usr/spark-2.4.1/volume


