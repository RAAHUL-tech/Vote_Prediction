# Vote_Prediction

Real Time Vote analysis using Kafka, Spark and HDFS

## Requirements
1. Python 3.8
2. Java 1.8
3. Apache Kafka
4. Spark 3.5.3
5. Hadoop 3

Clone the repository and create a virtual environment using the command ```python -m venv .venv```. To activate the virtual environment run ```.venv\Scripts\activate```. Run ```pip install -r requirements.txt``` to install necessary dependencies on your virtual environment.

## Phase 1 : Historical Data Analysis
Step 1: Start the hadoop cluster by running ```start-all.cmd``` on your terminal. 

Step 2: To generate the Voters and candidate data using randomuser API run the following commands, ```python generate_voters_data.py```, ```python generate_candidate_data.py```.

Step 3: To generate historical data for training the linear regression model run the following command, ```python generate_historical_data.py```. You can create historical data for different years.

Step 4: Upload voter_data, candidate_data and historical_data to HDFS.

Step 5: To train the linear regression model using the historical data in HDFS run the following command ```python linear_reg.py```. To evaluate the trained model run ```python linear_reg_test.py```. To visualize the prediction from the model run ```python linear_reg_plot.py```.

##Phase 2 : Real time Data Analysis without kafka integration
Step 1: Generate the Voters and candidate data using randomuser API run the following commands, ```python generate_voters_data.py```, ```python generate_candidate_data.py```.

Step 2: To train the ARIMA model run ```python arima.py```. To predict the future time frames run ```python arima_test.py```. To visualize the predicted time frames run ```python arima_plot.py```.

## Phase 2 : Real time Data Analysis with kafka integration
Step 1: Start the hadoop cluster by running ```start-all.cmd``` on your terminal. Start the kafka server by running ```.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties``` and ```.\bin\windows\kafka-server-start.bat .\config\server.properties``` in different terminals.

Step 2: Create necessary kafka topics by running the following commands, ```kafka-topics.bat --bootstrap-server localhost:9092 --create --topic voting_data``` , ```kafka-topics.bat --bootstrap-server localhost:9092 --create --topic data_<candidate_id>```.

Step 3: To ingest the data to the kafka topic in real-time run ```python kafka_producer.py```. Ensure you have created a kafka_topic 'voting_data'.

Step 4: To process the data from the kafka run ```python kafka_consumer.py```. This code processes each message and commits the offset. It does the face verification using Siamese networks and trains the ARIMA model with real-time data.

Step 5: To visualize the analysis using streamlit run ```streamlit run frontend.py```.
