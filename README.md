# Movie Rating Processor

This project is a Movie Rating Processor that includes Spark-based processing. Below are some useful commands for building, testing, and running the application.

## Prerequisites

Before running this application, ensure that you have the following installed:
- [JDK 1.8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
- [Scala 2.12](https://www.scala-lang.org/download/2.12.14.html)
- You need one of the following for executing the Spark Job:
    - [SBT](https://www.scala-sbt.org/download.html) for building and testing Scala projects.
    - [Docker](https://www.docker.com/get-started) for building and running Docker containers.
    - [Apache Spark](https://spark.apache.org/downloads.html) for Spark-based processing.


## Run Unit Test

To run the unit tests, use the following command:
```bash
sbt test
```

## Package

To package the application, use the following command:
```bash
sbt clean && sbt package
```

## Run Spark Job using SBT cmd
To start Spark processor using sbt
```bash
sbt "runMain com.dp.training.Main --input src/main/resources/input/ --output src/main/resources/output/"
```

## Build Docker Image

To build a Docker image for the Movie Rating Processor, use the following command:
```bash
docker build -t movie-rating-processor .
```

## Run Docker Container

To run a Docker container and get an interactive shell, use the following command:
* `--input` : Directory of the input files
* `--output` : Directory of the output files
```bash
docker run -it movie-rating-processor /bin/bash

# Once you are in the Docker shell
/opt/spark/bin/spark-submit --class com.dp.training.Main --master local[*] /app/movie-rating-processor.jar --input /app/input/ --output /tmp/output/

# List the Output directories.
ls -la /tmp/output/
```

## Spark Submit
To submit the Spark application, use the following command:
```bash
./spark-submit --class com.dp.training.Main --master local[*] /app/movie-rating-processor.jar --input /app/input/ --output /tmp/output/ 
```
* 

## Clean Docker Containers

To clean up Docker containers based on the image 'movie-rating-processor', use the following command:
```bash
docker rm $(docker ps -a -q -f ancestor=movie-rating-processor)
```

## Output
1. **agg_movie_ratings** - Parquet snappy-compressed data, routed to files based on hash(MovieID) using `bucketBy`.
2. **top_rated_movie_per_user** - Parquet snappy-compressed data containing the latest top-rated movies, with a limit of 3 movies per user. 
3. **movie** - Parquet snappy-compressed movie data records are routed to files based on the hash of MovieID using `bucketBy`. 
4. **rating** - Parquet snappy-compressed data, partitioned by the year of the event timestamp.  
5. **user** - Parquet snappy-compressed data with records routed to files based on hash(UserID) using `bucketBy`.

### Improvement
1. For advanced schema evolution, such as dynamic typing, AVRO is recommended as a superior file format.
2. When running a Spark job through sbt, you may encounter a warning message (Caused by: java.io.FileNotFoundException: hadoop-client-api No such file or directory). Although this is a warning message and does not impact code execution, it is advisable to investigate and implement a solution to address it.
3. To enhance security and address potential vulnerabilities, consider upgrading Spark to version 3.4.2 or later.
4. While it may be straightforward to partition rating data by the day or month of ingestion, the number of records was too few, resulting in smaller files. Hence, the chosen partition was based on the year of ingestion. 
5. Considering consumer queries that aggregate ratings by MovieId/UserId, the rating data could be further optimized for reading by adding a nested bucket for MovieId/UserId.
6. The Data Quality flag was added and populated to the input data. The next step would be to setup process to filter and process only the data which passes data quality checks   

### Assumptions
* There are no duplicate user ratings of a movie i.e. `ratings.groupBy("UserID","MovieID").filter(col("count") > 1).count` is always 1.