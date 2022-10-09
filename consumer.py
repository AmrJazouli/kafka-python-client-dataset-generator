
import sys
import pandas as pd
from sensors.measures import Measures
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from uncertainties import ufloat
import random
import numpy as np

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

    # Select the Kafka topic in confluenct cloud we will be consuming records from
    topic = "topic_0"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:

        # Initializes a dictionary of records consumed from the kafka topic, it contains 6 key-value pairs 
        records_dict = {key_dict: [] for key_dict in ['Clock CPU Core #1', 'Temperature CPU Package', 'Load CPU Total', 'Power CPU Package', 'Temperature GPU Core', 'Load GPU Core']}
        
        records_dict_max_values = {key_dict: [] for key_dict in ['Clock CPU Core #1', 'Temperature CPU Package', 'Load CPU Total', 'Power CPU Package', 'Temperature GPU Core', 'Load GPU Core']}
        

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
                value_parsed_1 = value.replace('[','')
                value_parsed_2 = value_parsed_1.replace(']','')
                                
                list_1 = value_parsed_2.split(",")              
                value_parsed_3=list(map(float,list_1))
                                                              
                records_dict[key].append(value_parsed_3[0])
                           

                
                # Condition to check if all values of record_dict_max_values have a length equals to 1
                if len(records_dict_max_values[key]) != 1:
                    records_dict_max_values[key].append(value_parsed_3[1])    
                    
                    
                # N equals to number of pushes, in this example, we make 100 pushes to the kafka topic
                #N = 12
                N = 100

                # Conversion of record_dict to list to perform some checks                   
                vals = records_dict.values()
                

                # This is a condition to check if all values of records_dict have the same length which is the number of pushes to the kafka topic
                # So that to have a square dataframe
                if all(len(item) == N for item in vals):
                    
                    #Creation of a square dataframe with keys names as column names
                    df_Ones = pd.DataFrame.from_dict(records_dict)

                    df_Zeros_2 = df_Ones.copy()

                    #Creation of a target value column, 1 means ok no technical intervention is needed, 0 means warning a potential malfunction exists
                    #For now, we merge the dataframe with target value column filled with 1
                    list_of_ones = [1] * N
                    
                    # Adds the target value column 7 to the dataframe
                    df_Ones.insert(6, "No Technical Intervention Required", list_of_ones)

                    
                    #Creation of a square dataframe with keys names as column names of Zeros
                    df_Zeros_1 = pd.DataFrame.from_dict(records_dict_max_values)

                                     
                    sizeDict = len(records_dict_max_values)

                    Nb_iter = int(N/sizeDict)    

                    
                    
                    print(sizeDict)
                    print()
                    print(Nb_iter)
                    print()

                    # Deviation values of the sensor types above
                    # They will be used for for generating random values within intervals

                    dev_Clock = 3
                    dev_TemperatureCPU = 5    
                    dev_PowerCPU = 3
                    dev_LoadCPU = 5
                    dev_LoadGPU = 0.5
                    dev_TemperatureGPU = 3

                    # Generating dataframes of random values around the thresold values

                    c_1 = records_dict_max_values['Clock CPU Core #1'][0]+dev_Clock
                    c_2 = records_dict_max_values['Clock CPU Core #1'][0]-dev_Clock                
                    rand_array_1 = [random.uniform(c_1,c_2) for i in range(Nb_iter)]
                    df1 = pd.DataFrame(rand_array_1,columns=['Clock CPU Core #1'])
                    

                    tc_1 = records_dict_max_values['Temperature CPU Package'][0]+dev_TemperatureCPU
                    tc_2 = records_dict_max_values['Temperature CPU Package'][0]-dev_TemperatureCPU                                 
                    rand_array_2 = [random.uniform(tc_1,tc_2) for i in range(Nb_iter)]
                    df2 = pd.DataFrame(rand_array_2,columns=['Temperature CPU Package'])
                                    
                    
                    lc_1 = records_dict_max_values['Load CPU Total'][0]+dev_PowerCPU
                    lc_2 = records_dict_max_values['Load CPU Total'][0]-dev_PowerCPU                                  
                    rand_array_3 = [random.uniform(lc_1,lc_2) for i in range(Nb_iter)]
                    df3 = pd.DataFrame(rand_array_3,columns=['Load CPU Total'])
                    
                    
                    pc_1 = records_dict_max_values['Power CPU Package'][0]+dev_LoadCPU
                    pc_2 = records_dict_max_values['Power CPU Package'][0]-dev_LoadCPU                                   
                    rand_array_4 = [random.uniform(pc_1,pc_2) for i in range(Nb_iter)]
                    df4 = pd.DataFrame(rand_array_4,columns=['Power CPU Package'])
                    

                    tg_1 = records_dict_max_values['Temperature GPU Core'][0]+dev_TemperatureGPU
                    tg_2 = records_dict_max_values['Temperature GPU Core'][0]-dev_TemperatureGPU                            
                    rand_array_5 = [random.uniform(tg_1,tg_2) for i in range(Nb_iter)]
                    df5 = pd.DataFrame(rand_array_5,columns=['Temperature GPU Core'])
                    

                    lg_1 = records_dict_max_values['Load GPU Core'][0]+dev_LoadGPU
                    lg_2 = records_dict_max_values['Load GPU Core'][0]-dev_LoadGPU                                   
                    rand_array_6 = [random.uniform(lg_1,lg_2) for i in range(Nb_iter)]
                    df6 = pd.DataFrame(rand_array_6,columns=['Load GPU Core'])
                                                         

                    # Assigning previous dataframes to the main dataframe with Zeros

                    df_Zeros_2.loc[:Nb_iter-1,'Clock CPU Core #1'] = pd.DataFrame.assign(df1['Clock CPU Core #1']).values                                                          
                    df_Zeros_2.loc[Nb_iter:2*Nb_iter-1,'Temperature CPU Package'] = pd.DataFrame.assign(df2['Temperature CPU Package']).values              
                    df_Zeros_2.loc[2*Nb_iter:3*Nb_iter-1,'Load CPU Total'] = pd.DataFrame.assign(df3['Load CPU Total']).values
                    df_Zeros_2.loc[3*Nb_iter:4*Nb_iter-1,'Power CPU Package'] = pd.DataFrame.assign(df4['Power CPU Package']).values
                    df_Zeros_2.loc[4*Nb_iter:5*Nb_iter-1,'Temperature GPU Core'] = pd.DataFrame.assign(df5['Temperature GPU Core']).values
                    df_Zeros_2.loc[5*Nb_iter:6*Nb_iter-1,'Load GPU Core'] = pd.DataFrame.assign(df6['Load GPU Core']).values                    
                    
                    # To be reviewed if N % 6 == 0
                    rem = N % sizeDict

                    #print(rem)    
                    if rem != 0:
                        cc_1 = records_dict_max_values['Clock CPU Core #1'][0]+dev_Clock
                        cc_2 = records_dict_max_values['Clock CPU Core #1'][0]-dev_Clock                                     
                        rand_array_7 = [random.uniform(cc_1,cc_2) for i in range(rem)]
                        df7 = pd.DataFrame(rand_array_7,columns=['Clock CPU Core #1']) 
                        
                        df_Zeros_2.loc[6*Nb_iter:2*N-1,'Clock CPU Core #1'] = pd.DataFrame.assign(df7['Clock CPU Core #1']).values
                    

                    list_of_zeros = [0] * N
                    
                    # Adds the target value column 7 to the dataframe
                    df_Zeros_2.insert(6, "No Technical Intervention Required", list_of_zeros)

                    
                                   
                    #print(df_Zeros_2)

                    # Concatenation of the two previous dataframes: df_Ones and df_Zeros
                    df_final = pd.concat([df_Ones, df_Zeros_2], ignore_index=True)
                    
                    print()
                    print(df_final)
                    

                    # Converts the dataframe to a csv file 
                    df_final.to_csv('datasetWithOnesAndZeros.csv')
                                    
                   
           
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


   