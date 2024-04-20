# Usar una imagen base de Python
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create a user with a specific user ID (non-root), and change ownership
RUN useradd -m -u 1000 myuser && chown -R myuser:myuser /app

# Specify the user to use when running the image
USER 1000

# Run app.py when the container launches
CMD ["python3", "service.py"]
