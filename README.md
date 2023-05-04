# stock_market_data_pipeline
Dagster data pipeline with an ML predictor for all securities data.

Note: The computer I developed this on has strong specs such as an NVidia 2070 Super, AMD Ryzen 3600 12 core, 16 GB ram. 
- Run time for the scripts, directly, was 3 minutes 46 seconds.
- Run time for the pipeline through Dagster was 10 minutes 06 seconds.
- Improvements can be made through the use of Iterator objects to reduce memory usage
- Functions can be broken down further for multiprocessing


Problem 1: 
- Incorporated multithreading due to I/O bound computations for loading csv files.
- Used multiprocessing and mapping to transform dataframes prior to concatenating them.
- downcasted dtypes to reduce memory consumption
- Used the pyarrow engine for pandas to read csv files quicker
- saved the file as a parquet

Problem 2:
- Note: Dask has a bug with the groupby().rolling() functions which I will be opening an issue for. For this project I only used standard pandas.
- The importing of a large dataframe could've been split into partitions and have calculations done on each. However, due to time constraints I was not able to incorporate this methodology. Originally my plan was to use Dask.Dataframe but the bugs I encountered made this less likely. 
- I could have also chosen to use Pyspark for optimizing computations but I did not.

- This is rather bad practice but I didn't incorporate unit testing as I've done this work before. NEVER DO THIS AT HOME! I'm a huge believer in test-driven-development and Uncle Bob's methodologies for clean code. Were I developing an actual application I would've spent the time testing everything and breaking down all of my large functions.

Problem 3:
- I decided to use XGBooster as I wanted to take advantage of my GPU and speed up computations.
- I was also able to decrease memory consumption significantly which is important as Sklearn's tree regression algorithms can cause memory errors due to the number of trees being produced.

Problem 4: Use  
`GET http://rockysn.pythonanywhere.com/predict?vol_moving_avg=51318&adj_close_rolling_med=216`
and change the values as you'd like.

- Tested the API using Postman and kept it as barebones as possible.
- Should use an async for requests. Note that this server can easily be overloaded and crash.


Pipeline diagram:
- The pipeline might seem completely linear but it doesn't display the different ops involved in computing.

![Pipeline](pipeline.PNG)

Credit: 
- XGBoost documentation
- Pandas documentation
- Dask.Dataframe documentation + github
- logs provided in dagster_log.txt



