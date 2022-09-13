#!/usr/bin/env python

import sys
import pandas as pd
from sensors.measures import Measures
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    #topic = "topic_0"
    topic = "topic_0"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        # Initializes a dictionary of records consumed from the kafka topic, it contains 6 key-value pairs 
        records_dict = {key_dict: [] for key_dict in ['ClockCPUCoreOne', 'TemperatureCPUPackage', 'LoadCPUTotal', 'PowerCPUPackage', 'TemperatureGPUCore', 'LoadGPUCore']}

        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the key and value and append the value to the corresponding list
                topic=msg.topic()              
                key=msg.key().decode('utf-8')
                value=msg.value().decode('utf-8')
                records_dict[key].append(value)
                           
               
                # N equals to number of pushes, in this example, we make 100 pushes to the kafka topic
                N = 100
                                   
                vals = records_dict.values()
                # This is a condition to check if all values of records_dict have the same length which is the number of pushes to the kafka topic
                # So that to have a square dataframe
                if all(len(item) == N for item in vals):
                    
                    #Creation of a square dataframe with keys names as column names
                    df = pd.DataFrame.from_dict(records_dict)

                    #Creation of a target value column, 1 means ok no technical intervention is needed, 0 means warning a potential malfunction exists
                    #For now, we merge the dataframe with target value column filled with 1
                    list_of_ones = [1] * N
                    
                    # Adds the target value column 7 to the dataframe
                    df.insert(6, "NoTechnicalInterventionRequired", list_of_ones)

                    # Converts the dataframe to a csv file 
                    df.to_csv('datasetWithOnes.csv')
                                    
           
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


   