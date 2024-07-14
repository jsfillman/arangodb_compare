# Use the smallest possible base image that can run Python
FROM python:3.9.19-alpine3.20

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY arangodb_compare/ /app/arangodb_compare

# Set environment variables
ENV ARANGO_URL1=http://arangodb:8529
ENV ARANGO_USERNAME1=root
ENV ARANGO_PASSWORD1=testpassword
ENV ARANGO_DB_NAME1=_system

ENV ARANGO_URL2=http://arangodb:8530
ENV ARANGO_USERNAME2=root
ENV ARANGO_PASSWORD2=testpassword
ENV ARANGO_DB_NAME2=_system

ENV LOGFILE_OUT=/logs

# Copy and set the entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
