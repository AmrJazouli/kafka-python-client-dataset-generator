# kafka_python_client_dataset_generator

  A python client that will produce hardware data such as clock, temperature, Load and Power of the CPU and GPU into a kafka topic.
  
  The client consumes the data from kafka and generates a csv file.


  Prerequisites:

   OpenHardwareMonitor must be running before launching the batch files of producing and consuming.
   https://openhardwaremonitor.org/

   A subscription for a kafka topic in confluent cloud is necessary as well. 
   https://www.confluent.io/confluent-cloud/
