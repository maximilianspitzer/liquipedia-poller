from datetime import datetime
import psycopg2
from psycopg2 import sql
import logging
import os
from config import *

logger = logging.getLogger(__name__)

MIGRATIONS = [
    {
        'version': 1,
        'description': 'Initial schema creation',
        'up': """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS teams (
                team_id SERIAL PRIMARY KEY,
                team_name VARCHAR(200) UNIQUE NOT NULL,
                region VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tournaments (
                tournament_id SERIAL PRIMARY KEY,
                name VARCHAR(300) UNIQUE NOT NULL,
                date DATE NOT NULL,
                region VARCHAR(100) NOT NULL,
                status VARCHAR(50) DEFAULT 'upcoming',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (status IN ('upcoming', 'ongoing', 'completed'))
            );

            CREATE TABLE IF NOT EXISTS matches (
                match_id SERIAL PRIMARY KEY,
                tournament_id INTEGER REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
                team1_id INTEGER REFERENCES teams(team_id),
                team2_id INTEGER REFERENCES teams(team_id),
                winner_id INTEGER REFERENCES teams(team_id),
                score VARCHAR(50),
                stage VARCHAR(200),
                match_date TIMESTAMP,
                status VARCHAR(50) DEFAULT 'scheduled',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tournament_id, team1_id, team2_id, match_date),
                CHECK (status IN ('scheduled', 'ongoing', 'completed', 'cancelled'))
            );

            CREATE TABLE IF NOT EXISTS team_points (
                id SERIAL PRIMARY KEY,
                team_id INTEGER REFERENCES teams(team_id) ON DELETE CASCADE,
                tournament_id INTEGER REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
                points INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(team_id, tournament_id)
            );

            CREATE TABLE IF NOT EXISTS match_dependencies (
                dependency_id SERIAL PRIMARY KEY,
                source_match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
                target_match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
                position INTEGER CHECK (position IN (1, 2)),
                dependency_type VARCHAR(50) CHECK (dependency_type IN ('winner', 'loser')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_match_id, target_match_id)
            );

            -- Create update trigger function
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $func$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $func$ LANGUAGE plpgsql;

            -- Create triggers for all tables
            DROP TRIGGER IF EXISTS update_teams_updated_at ON teams;
            CREATE TRIGGER update_teams_updated_at
                BEFORE UPDATE ON teams
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            DROP TRIGGER IF EXISTS update_tournaments_updated_at ON tournaments;
            CREATE TRIGGER update_tournaments_updated_at
                BEFORE UPDATE ON tournaments
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            DROP TRIGGER IF EXISTS update_matches_updated_at ON matches;
            CREATE TRIGGER update_matches_updated_at
                BEFORE UPDATE ON matches
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            DROP TRIGGER IF EXISTS update_team_points_updated_at ON team_points;
            CREATE TRIGGER update_team_points_updated_at
                BEFORE UPDATE ON team_points
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            -- Create indexes
            CREATE INDEX IF NOT EXISTS idx_matches_tournament_id ON matches(tournament_id);
            CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
            CREATE INDEX IF NOT EXISTS idx_team_points_team_id ON team_points(team_id);
            CREATE INDEX IF NOT EXISTS idx_tournaments_region_date ON tournaments(region, date);
        """,
        'down': """
            DROP TABLE IF EXISTS match_dependencies;
            DROP TABLE IF EXISTS team_points;
            DROP TABLE IF EXISTS matches;
            DROP TABLE IF EXISTS tournaments;
            DROP TABLE IF EXISTS teams;
            DROP TABLE IF EXISTS schema_migrations;
        """
    },
    # Add more migrations here as needed
]

def get_current_version(cursor):
    """Get the current database schema version"""
    try:
        cursor.execute("""
            SELECT MAX(version) FROM schema_migrations
        """)
        version = cursor.fetchone()[0]
        return version or 0
    except psycopg2.Error:
        return 0

def run_migrations(conn):
    """Run all pending migrations"""
    cur = None
    try:
        # Force autocommit off initially
        conn.set_session(autocommit=False)
        cur = conn.cursor()
        
        # First ensure the migrations table exists in its own transaction
        cur.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT FROM pg_tables 
                    WHERE schemaname = 'public' 
                    AND tablename = 'schema_migrations'
                ) THEN
                    CREATE TABLE schema_migrations (
                        version INTEGER PRIMARY KEY,
                        description TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                END IF;
            END $$;
        """)
        conn.commit()
        
        # Get current version
        cur.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
        current_version = cur.fetchone()[0]
        logger.info(f"Current database version: {current_version}")
        
        # Run each migration in its own transaction
        for migration in MIGRATIONS:
            version = migration['version']
            if version > current_version:
                logger.info(f"Running migration {version}: {migration['description']}")
                try:
                    # Run migration statements one by one
                    statements = [s.strip() for s in migration['up'].split(';') if s.strip()]
                    for statement in statements:
                        if statement:
                            # Fix the trigger function syntax
                            if 'CREATE OR REPLACE FUNCTION update_updated_at_column()' in statement:
                                statement = """
                                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                                    RETURNS TRIGGER AS $func$
                                    BEGIN
                                        NEW.updated_at = CURRENT_TIMESTAMP;
                                        RETURN NEW;
                                    END;
                                    $func$ LANGUAGE plpgsql;
                                """
                            cur.execute(statement)
                    
                    # Record successful migration
                    cur.execute("""
                        INSERT INTO schema_migrations (version, description)
                        VALUES (%s, %s)
                    """, (version, migration['description']))
                    
                    conn.commit()
                    logger.info(f"Successfully applied migration {version}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error applying migration {version}: {e}")
                    raise
        
        logger.info("All migrations completed successfully")
        
    except Exception as e:
        if conn and not conn.closed:
            conn.rollback()
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn and not conn.closed:
            conn.set_session(autocommit=True)