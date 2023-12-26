# Use an official Apache Spark runtime as a base image
FROM apache/spark:latest

# Set the working directory in the container
WORKDIR /app

# Copy the local code to the container
COPY . /app

# Switch to root user to install dependencies
USER root

# Install any dependencies or configurations needed
# For example, if you need to install additional Python packages, you can use pip
RUN pip install -r requirements.txt

# Switch back to the non-root user (assuming spark is the default user in the base image)
USER spark

# Set environment variables, if necessary
ENV SPARK_HOME=/opt/spark

# Specify the command to run your application
CMD ["spark-submit", "--master", "local", "application.py", "--inline", "--plan", "resources/configuration-local-csv-to-jdbc.json"]
