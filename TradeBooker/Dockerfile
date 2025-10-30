FROM python:3.11

# Set the working directory inside the container to /app
WORKDIR /app

#First copy requirement.txt
COPY requirements.txt .

# Install Python dependencies listed in requirements.txt
RUN pip install --no-cache-dir --upgrade pip -r requirements.txt

# Copy the rest of the current directory contents into the container at /app
COPY . .

EXPOSE 8000