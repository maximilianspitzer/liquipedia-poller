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
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql';

            -- Create triggers for all tables
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
    with conn.cursor() as cur:
        try:
            # Start transaction
            conn.autocommit = False
            
            # Get current version
            current_version = get_current_version(cur)
            logger.info(f"Current database version: {current_version}")
            
            # Run pending migrations
            for migration in MIGRATIONS:
                version = migration['version']
                if version > current_version:
                    logger.info(f"Running migration {version}: {migration['description']}")
                    try:
                        # Run migration
                        cur.execute(migration['up'])
                        
                        # Record migration
                        cur.execute("""
                            INSERT INTO schema_migrations (version, description)
                            VALUES (%s, %s)
                        """, (version, migration['description']))
                        
                        logger.info(f"Successfully applied migration {version}")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error applying migration {version}: {e}")
                        raise
            
            # Commit transaction
            conn.commit()
            logger.info("All migrations completed successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            conn.autocommit = True