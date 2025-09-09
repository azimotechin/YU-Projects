FROM python:3.11

# Set the working directory inside the container to /app
WORKDIR /app

# First copy requirement.txt
COPY requirements.txt .

# Install Python dependencies listed in requirements.txt
RUN pip install --no-cache-dir --upgrade pip -r requirements.txt

# Copy all files from app/ directory to /app/
COPY . ./

# Expose all possible ports
EXPOSE 5000 8080 8501

# Set default environment variable
ENV SERVICE_TYPE=streamlit

# Use main.py as the entry point to route between services
CMD ["python", "main.py"]