# Use the official Python 3.9 Alpine image as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Command to run the application
ENTRYPOINT ["python", "app/problem1/src/main.py"]