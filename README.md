# Kafka_Dataset_Generator

  A python client that will produce hardware data such as clock, temperature, Load and Power of the CPU and GPU into a kafka topic.
  
  The client consumes the data from kafka and generates a csv file. It also adds a target column called 'No Technical Intervention Required' in the generated dataset which can be used for a machine learning purpose.


  Prerequisites:

   OpenHardwareMonitor software must be running before launching the batch files of producing and consuming.
   https://openhardwaremonitor.org/

   A subscription for a kafka topic in confluent cloud is necessary as well. 
   https://www.confluent.io/confluent-cloud/


  The following batch can take up to 10 minutes to complete.

  In the command line:

  1. Go to the kafka_python_client_dataset_generator project folder:
     cd C:\Users\...\kafka_python_client_dataset_generator
  2. Activate your virtual environement:
     conda activate venv
  3. Run the batchProducerConsumerKafka batch file as shown following: 
     C:\Users\...\kafka_python_client_dataset_generator>batchProducerConsumerKafka.bat
  4. When the csv file is generated, press CTRL+C to stop the consumer. Then, press 'Y' to terminate the batch.
