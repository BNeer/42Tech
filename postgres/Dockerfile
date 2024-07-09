# Use the official PostgreSQL image
FROM postgres:12

# Set environment variables
ENV POSTGRES_DB techdb
ENV POSTGRES_USER techuser
ENV POSTGRES_PASSWORD TechUser@2024

# Expose the PostgreSQL port
EXPOSE 5432

# Copy initialization scripts if needed (for schema/data)
COPY init.sql /docker-entrypoint-initdb.d/
