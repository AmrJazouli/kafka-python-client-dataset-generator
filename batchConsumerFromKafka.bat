@ECHO OFF

ECHO This is a batch file to consume records from the kafka topic, this will generate a csv file containing 100 rows  
ECHO
ECHO

python consumer.py getting_started.ini

PAUSE