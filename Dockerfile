FROM apache/airflow:2.9.1

USER root

# Install Node.js and build tools
RUN apt-get update && apt-get install -y curl build-essential && \
    curl -sL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt /requirements.txt

USER airflow

# Install Python libraries
RUN pip install --no-cache-dir -r /requirements.txt