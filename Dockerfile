# Use an official Python runtime as a parent image
FROM python:3.8.10

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
# Set PYTHONUNBUFFERED to prevent Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED 1

# Run app.py when the container launches
CMD ["python", "skeleton.py"]