#!/bin/sh

echo "Waiting for PostgreSQL to be ready..."
# Wait for PostgreSQL to be ready with password from environment
until PGPASSWORD="$DB_PASSWORD" pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done

echo "PostgreSQL is up - checking if database exists"

# Try to create database if it doesn't exist
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '$DB_NAME') THEN
            CREATE DATABASE $DB_NAME;
        END IF;
    END
    \$\$;
EOSQL

echo "Database setup complete - starting service"
# Run the service
python service.py