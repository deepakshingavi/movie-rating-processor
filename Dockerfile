# Use a base Spark image
FROM apache/spark:v3.3.0

# Copy the input files
COPY src/main/resources/input /app/input


# Copy the JAR file built by SBT
COPY target/scala-2.12/movie-rating-processor_2.12-1.0.jar /app/movie-rating-processor.jar

# Set the working directory
WORKDIR /app

# Create output directory and set permissions
RUN mkdir /tmp/output && chmod +w /tmp/output

# Command to run your Spark application
CMD [ \
     "/opt/spark/bin/spark-submit", \
     "--class", "com.dp.training.Main", \
     "--master", "local[*]", \
     "/app/movie-rating-processor.jar", \
     "--input", "/app/input/", \
     "--output", "/tmp/output/" \
     ]
