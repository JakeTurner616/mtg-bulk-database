# Dockerfile
FROM postgres:15

# Copy the SQL initialization script into the container so that it runs on first startup.
COPY init.sql /docker-entrypoint-initdb.d/

# Expose PostgreSQL default port.
EXPOSE 5432

# Declare a volume for persistent database storage.
VOLUME [ "/var/lib/postgresql/data" ]
