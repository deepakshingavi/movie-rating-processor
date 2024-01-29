# Movie Rating Processor

This project is a Movie Rating Processor that includes Spark-based processing. Below are some useful commands for building, testing, and running the application.

## Run Unit Test

To run the unit tests, use the following command:
```bash
sbt test
```

## Package

To package the application, use the following command:
```bash
sbt package
```

## Build Docker Image

To build a Docker image for the Movie Rating Processor, use the following command:
```bash
docker build -t movie-rating-processor .
```

## Run Docker Container

To run a Docker container and get an interactive shell, use the following command:
```bash
docker run -it movie-rating-processor /bin/bash
```

## Spark Submit

To submit the Spark application, use the following command:
```bash
/opt/spark/bin/spark-submit --class com.dp.training.Main --master local[*] /app/movie-rating-processor.jar --input /app/input/ --output /tmp/output/ 
```

## Clean Docker Containers

To clean up Docker containers based on the image 'movie-rating-processor', use the following command:
```bash
docker rm $(docker ps -a -q -f ancestor=movie-rating-processor)
```

## Output
1. **agg_movie_ratings** - Parquet snappy-compressed data, routed to files based on hash(MovieID) using `bucketBy`.
2. **top_rated_movie_per_user** - Parquet snappy-compressed data containing the latest top-rated movies, with a limit of 3 movies per user. 
3. **movie** - Parquet snappy-compressed movie data records are routed to files based on the hash of MovieID using `bucketBy`. 
4. **rating** - Parquet snappy-compressed data, partitioned by the year of the event timestamp, with records routed to nested bucket files based on hash(MovieID, UserID) using `bucketBy`.  
5. **user** - Parquet snappy-compressed data with records routed to files based on hash(UserID) using `bucketBy`.
   
*Note : The base output directory, denoted as src/main/resources/output/, serves as the primary location where the generated or processed data is stored.*