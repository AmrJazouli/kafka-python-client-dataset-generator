@ECHO OFF
ECHO.
ECHO.
ECHO ---------- Batch to produce and consume sensors data to and from kafka topic will start ( takes approximately 5 to 10 minutes to complete ) ----------
ECHO.
ECHO.
ECHO Producing 100 pushes to the kafka topic containing subsets of records: 
ECHO.
ECHO.


for /L %%n in (1,1,100) do call :producer_function %%n
ECHO.
ECHO.
ECHO ********* 100 pushes produced successfully *********
ECHO.
ECHO.
ECHO Consuming records from the kafka topic, this will generate a csv file containing 100 rows: 
ECHO.
ECHO.

python consumer.py config_consumer.ini

EXIT /B %ERRORLEVEL%

:producer_function
ECHO.
ECHO Push Number %~1
ECHO.
python producer.py
EXIT /B 0


PAUSE

