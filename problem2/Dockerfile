# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install PySpark
RUN pip install pyspark

# Run spark_app.py when the container launches
ENTRYPOINT ["python", "app/problem2/src/main.py"]