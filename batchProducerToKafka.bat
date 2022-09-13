@ECHO OFF

ECHO This is a batch file to produce 100 pushes containing subset of records to the kafka topic 
ECHO
ECHO

for /L %%n in (1,1,100) do python producer.py getting_started.ini
	

PAUSE