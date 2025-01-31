# Use an official Python image as a base
FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y python3 python3-pip

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt requirements.txt

# Install required Python libraries
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Command to run the PySpark application
CMD ["python3", "main_app.py"]