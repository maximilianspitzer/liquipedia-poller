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
        'description': 'Initial schema creation and triggers',
        'up': """
            -- Create the trigger function first
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            -- Create tables
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

            -- Create indexes
            CREATE INDEX IF NOT EXISTS idx_matches_tournament_id ON matches(tournament_id);
            CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
            CREATE INDEX IF NOT EXISTS idx_team_points_team_id ON team_points(team_id);
            CREATE INDEX IF NOT EXISTS idx_tournaments_region_date ON tournaments(region, date);

            -- Create triggers
            CREATE TRIGGER update_teams_updated_at
                BEFORE UPDATE ON teams
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            CREATE TRIGGER update_tournaments_updated_at
                BEFORE UPDATE ON tournaments
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            CREATE TRIGGER update_matches_updated_at
                BEFORE UPDATE ON matches
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();

            CREATE TRIGGER update_team_points_updated_at
                BEFORE UPDATE ON team_points
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
        """,
        'down': """
            DROP TRIGGER IF EXISTS update_teams_updated_at ON teams;
            DROP TRIGGER IF EXISTS update_tournaments_updated_at ON tournaments;
            DROP TRIGGER IF EXISTS update_matches_updated_at ON matches;
            DROP TRIGGER IF EXISTS update_team_points_updated_at ON team_points;
            DROP FUNCTION IF EXISTS update_updated_at_column();
            DROP TABLE IF EXISTS match_dependencies;
            DROP TABLE IF EXISTS team_points;
            DROP TABLE IF EXISTS matches;
            DROP TABLE IF EXISTS tournaments;
            DROP TABLE IF EXISTS teams;
            DROP TABLE IF EXISTS schema_migrations;
        """
    }
]

def get_current_version(cursor):
    """Get the current database schema version"""
    try:
        cursor.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
        return cursor.fetchone()[0]
    except psycopg2.Error:
        return 0

def run_migrations(conn):
    """Run all pending migrations"""
    cur = None
    try:
        # Set autocommit to True before any operations
        conn.set_session(autocommit=True)
        
        cur = conn.cursor()
        current_version = get_current_version(cur)
        logger.info(f"Current database version: {current_version}")
        
        # Set autocommit to False for the migration transaction
        conn.set_session(autocommit=False)
        
        for migration in MIGRATIONS:
            version = migration['version']
            if version > current_version:
                try:
                    logger.info(f"Running migration {version}: {migration['description']}")
                    # Execute the SQL string directly
                    cur.execute(migration['up'])
                    cur.execute("INSERT INTO schema_migrations (version, description) VALUES (%s, %s)",
                              (version, migration['description']))
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
            
        # Set autocommit back to True after all operations
        if conn and not conn.closed:
            conn.set_session(autocommit=True)