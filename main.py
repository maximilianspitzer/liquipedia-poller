import os
import sys
import time
import re
import logging
import threading
from bs4 import BeautifulSoup
import requests
import psycopg2
from psycopg2 import sql, pool
from dotenv import load_dotenv
from datetime import datetime
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from threading import Lock
from config import *
from exceptions import *
from schema import Team, Tournament, Match, MatchDependency

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabasePool:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabasePool, cls).__new__(cls)
                    cls._instance._initialize_pool()
        return cls._instance
    
    def _initialize_pool(self):
        try:
            self.pool = pool.ThreadedConnectionPool(
                DB_POOL_MIN_CONN,
                DB_POOL_MAX_CONN,
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            logger.info("Connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing connection pool: {e}")
            raise DatabaseError(f"Failed to initialize connection pool: {e}")

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"Error getting connection from pool: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)

    def close(self):
        """Close all connections in the pool"""
        if hasattr(self, 'pool'):
            self.pool.closeall()
            logger.info("Connection pool closed")

# Global database pool instance
db_pool = DatabasePool()

@contextmanager
def get_db_connection(dbname='postgres'):
    """Context manager for database connections with improved error handling"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            # Add connection pooling and timeout settings
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            connect_timeout=10
        )
        conn.autocommit = True
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except psycopg2.Error as e:
                logger.error(f"Error closing database connection: {e}")

def create_database():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Check if database exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (os.getenv('DB_NAME'),))
            exists = cur.fetchone()
            
            if exists:
                # Drop existing connections and database
                cur.execute("""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                    AND pid <> pg_backend_pid()
                """, (os.getenv('DB_NAME'),))
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(os.getenv('DB_NAME'))
                ))
            
            # Create the database
            cur.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(os.getenv('DB_NAME'))
            ))
            print(f"Database {os.getenv('DB_NAME')} created successfully")
    except Exception as e:
        print(f"Error creating database: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def create_database_connection():
    """Create database connection with retry mechanism"""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            connect_timeout=DB_CONNECT_TIMEOUT
        )
        logger.info("Successfully established database connection")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise DatabaseError(f"Failed to connect to database: {e}") from e
    except Exception as e:
        logger.error(f"Unexpected error during database connection: {e}")
        raise

def create_tables(conn):
    """Create database tables if they don't exist and handle schema updates"""
    with conn.cursor() as cur:
        try:
            # Begin transaction
            conn.autocommit = False
            
            # Create tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    team_id SERIAL PRIMARY KEY,
                    team_name VARCHAR(200) UNIQUE NOT NULL,
                    region VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tournaments (
                    tournament_id SERIAL PRIMARY KEY,
                    name VARCHAR(300) UNIQUE NOT NULL,
                    date DATE NOT NULL,
                    region VARCHAR(100) NOT NULL,
                    status VARCHAR(50) DEFAULT 'upcoming',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CHECK (status IN ('upcoming', 'ongoing', 'completed'))
                )
            """)
            
            cur.execute("""
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
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS team_points (
                    id SERIAL PRIMARY KEY,
                    team_id INTEGER REFERENCES teams(team_id) ON DELETE CASCADE,
                    tournament_id INTEGER REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
                    points INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(team_id, tournament_id)
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS match_dependencies (
                    dependency_id SERIAL PRIMARY KEY,
                    source_match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
                    target_match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
                    position INTEGER CHECK (position IN (1, 2)),
                    dependency_type VARCHAR(50) CHECK (dependency_type IN ('winner', 'loser')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_match_id, target_match_id)
                )
            """)
            
            # Create update trigger function if it doesn't exist
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ language 'plpgsql'
            """)
            
            # Create triggers for all tables
            for table in ['teams', 'tournaments', 'matches', 'team_points']:
                cur.execute(f"""
                    DROP TRIGGER IF EXISTS update_{table}_updated_at ON {table};
                    CREATE TRIGGER update_{table}_updated_at
                    BEFORE UPDATE ON {table}
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
                """)
            
            # Create indexes for better query performance
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_matches_tournament_id ON matches(tournament_id);
                CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
                CREATE INDEX IF NOT EXISTS idx_team_points_team_id ON team_points(team_id);
                CREATE INDEX IF NOT EXISTS idx_tournaments_region_date ON tournaments(region, date);
            """)
            
            conn.commit()
            logger.info("Database tables and triggers created/updated successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating database tables: {e}")
            raise DatabaseError(f"Failed to create database tables: {e}")
        finally:
            conn.autocommit = True

class ExponentialBackoff:
    """Implements exponential backoff algorithm for retries"""
    def __init__(self, initial_delay: float = 1.0, max_delay: float = 60.0, multiplier: float = 2.0):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.current_delay = initial_delay
        self.attempts = 0

    def reset(self):
        """Reset the backoff to initial state"""
        self.current_delay = self.initial_delay
        self.attempts = 0

    def get_next_delay(self) -> float:
        """Get the next delay interval"""
        delay = min(self.current_delay * (self.multiplier ** self.attempts), self.max_delay)
        self.attempts += 1
        return delay

class RateLimiter:
    def __init__(self, requests_per_minute=30):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = {}  # Dictionary for multiple domains
        self.backoff = {}  # Dictionary of ExponentialBackoff instances per domain
        self.lock = Lock()  # Thread-safe lock

    def _get_backoff(self, domain: str) -> ExponentialBackoff:
        """Get or create backoff instance for domain"""
        if domain not in self.backoff:
            self.backoff[domain] = ExponentialBackoff()
        return self.backoff[domain]

    def wait(self, domain='default'):
        """Wait if necessary to respect rate limits for specific domain"""
        with self.lock:
            current_time = time.time()
            backoff = self._get_backoff(domain)
            
            if domain in self.last_request_time:
                elapsed = current_time - self.last_request_time[domain]
                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    logger.debug(f"Rate limiting {domain}: sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
            
            self.last_request_time[domain] = time.time()

    def handle_429(self, domain='default'):
        """Handle rate limit exceeded (HTTP 429)"""
        with self.lock:
            backoff = self._get_backoff(domain)
            delay = backoff.get_next_delay()
            logger.warning(f"Rate limit exceeded for {domain}. Backing off for {delay:.2f} seconds")
            time.sleep(delay)

    def reset(self, domain='default'):
        """Reset rate limiter and backoff for a domain"""
        with self.lock:
            if domain in self.last_request_time:
                del self.last_request_time[domain]
            if domain in self.backoff:
                self.backoff[domain].reset()

# Create global rate limiter instance with configuration
rate_limiter = RateLimiter(requests_per_minute=REQUESTS_PER_MINUTE)

def fetch_page_content(url, max_retries=3, retry_delay=5):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    domain = 'liquipedia.net'
    for attempt in range(max_retries):
        try:
            rate_limiter.wait(domain)  # Use domain-specific rate limiting
            full_url = f"{BASE_URL}{url}"
            logger.info(f"Fetching {full_url} (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(full_url, headers=headers, timeout=REQUEST_TIMEOUT)
            logger.info(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                content = response.text
                logger.debug(f"Content length: {len(content)} bytes")
                
                soup = BeautifulSoup(content, 'html.parser')
                main_content = soup.find('div', class_='mw-parser-output')
                
                if main_content:
                    logger.debug(f"Found main content area with {len(main_content.find_all('div'))} div elements")
                    return content
                else:
                    logger.warning("Main content area not found in response")
                    
            elif response.status_code == 429:  # Too Many Requests
                rate_limiter.handle_429(domain)  # Handle rate limit exceeded
                continue
                
            elif response.status_code == 404:
                logger.error(f"Page not found: {full_url}")
                return None
                
            else:
                logger.error(f"Failed to fetch page. Status code: {response.status_code}")
                
        except requests.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
                
        except Exception as e:
            logger.error(f"Unexpected error during page fetch: {str(e)}")
            break
            
    raise ScrapingError(f"Failed to fetch page after {max_retries} attempts: {url}")

def parse_match_date(date_str):
    """Parse date strings from Liquipedia"""
    try:
        # Remove timezone abbreviations
        clean_date = date_str
        for tz in ['CEST', 'CET', 'EDT', 'EST', 'UTC']:
            clean_date = clean_date.replace(tz, '').strip()
        
        # Try parsing with time first
        try:
            return datetime.strptime(clean_date, '%B %d, %Y - %H:%M')
        except ValueError:
            # If that fails, try parsing just the date
            return datetime.strptime(clean_date, '%B %d, %Y')
    except Exception as e:
        print(f"Error parsing date {date_str}: {e}")
        # Return the first day of the tournament's month instead of current time
        if ',' in date_str:
            try:
                month_year = ' '.join(date_str.split(',')[0].split()[:-1] + [date_str.split(',')[1].strip()])
                return datetime.strptime(month_year, '%B %Y')
            except:
                pass
        return datetime.now()

def extract_team_name(raw_name):
    """Clean team names by removing team handles that appear at the end in CAPS"""
    if not raw_name or 'TBD' in raw_name:
        return None
        
    # Known team prefixes that have special cleaning rules
    team_prefixes = {
        'SKCalalas': lambda x: 'SKCalalas NA' if 'NASKC' in x else x,
    }
    
    # Known teams that should not be modified at all
    no_clean_teams = {
        'Tribe Gaming',
        'FUT Esports'
    }
    
    name = raw_name.strip()
    
    # Check for exact matches first
    if name in no_clean_teams:
        return name
        
    # Check for team prefixes with special rules
    for prefix, rule in team_prefixes.items():
        if name.startswith(prefix):
            return rule(name)
    
    # Default cleaning: remove any CAPS sequence at the end of the name
    match = re.search(r'^(.*?)([A-Z]{2,})$', name)
    if match:
        return match.group(1).strip()
            
    return name

def parse_score(score_elem):
    """Extract and validate score from element"""
    if not score_elem:
        return '-'
    score = score_elem.get_text(strip=True)
    return score if score else '-'

def get_stage_info(match_elem, match_info, match_idx=0):
    """Determine the tournament stage for a match based on bracket structure"""
    # Look for brkts-round element to determine round position
    round_elem = match_elem.find_parent('div', class_='brkts-round')
    if round_elem:
        # Find how many rounds there are in total
        all_rounds = round_elem.find_parent().find_all('div', class_='brkts-round')
        total_rounds = len(all_rounds)
        current_round = all_rounds.index(round_elem)
        
        # In reverse order: final round is the last one, semis second to last, etc.
        if current_round == total_rounds - 1:
            return "Grand Final"
        elif current_round == total_rounds - 2:
            return "Semi-finals"
        else:
            return "Quarter-finals"
    
    # Fallback to index-based detection if round structure is not found
    if match_idx < 4:
        return "Quarter-finals"
    elif match_idx < 6:
        return "Semi-finals"
    else:
        return "Grand Final"

def extract_match_data(soup, tournament_id):
    matches = []
    dependencies = []
    print("\nSearching for match data...")
    
    main_content = soup.find('div', class_='mw-parser-output')
    if not main_content:
        print("Could not find main content area")
        return matches, dependencies

    # Find all bracket containers first
    bracket_containers = main_content.find_all('div', class_='brkts-bracket')
    if not bracket_containers:
        print("No bracket containers found")
        return matches, dependencies

    for bracket in bracket_containers:
        match_elements = bracket.find_all('div', class_='brkts-match')
        if not match_elements:
            continue

        print(f"Found {len(match_elements)} matches in bracket")
        
        # First pass: collect all matches
        for match_idx, match in enumerate(match_elements):
            try:
                team1_elem = match.find('div', class_=lambda x: x and 'brkts-opponent-entry' in x and not 'last' in x)
                team2_elem = match.find('div', class_=lambda x: x and 'brkts-opponent-entry' in x and 'last' in x)
                
                if not (team1_elem and team2_elem):
                    continue

                # Extract match data
                team1_name = extract_team_name(team1_elem.find('div', class_='block-team').get_text(strip=True))
                team2_name = extract_team_name(team2_elem.find('div', class_='block-team').get_text(strip=True))
                
                if not team1_name or not team2_name:
                    continue
                    
                score1 = parse_score(team1_elem.find('div', class_='brkts-opponent-score-inner'))
                score2 = parse_score(team2_elem.find('div', class_='brkts-opponent-score-inner'))
                
                is_scheduled = score1 == '-' or score2 == '-'
                status = 'scheduled' if is_scheduled else 'completed'
                
                winner = None
                if not is_scheduled:
                    try:
                        score1_num = int(score1)
                        score2_num = int(score2)
                        winner = team1_name if score1_num > score2_num else team2_name if score2_num > score2_num else None
                    except ValueError:
                        pass
                
                match_info = match.find('div', class_='brkts-popup')
                match_date = datetime.now()
                
                if match_info and (date_elem := match_info.find('div', class_='match-countdown-block')):
                    match_date = parse_match_date(date_elem.get_text(strip=True))
                
                stage = get_stage_info(match, match_info, match_idx)  # Pass match_idx here
                
                match_data = {
                    'team1': team1_name,
                    'team2': team2_name,
                    'winner': winner,
                    'score': f"{score1}-{score2}",
                    'stage': stage,
                    'match_date': match_date,
                    'status': status,
                    'bracket_index': match_idx
                }
                matches.append(match_data)
                print(f"Found {status} match ({stage}): {team1_name} vs {team2_name} ({score1}-{score2})")
            
            except Exception as e:
                print(f"Error processing match: {e}")
                import traceback
                traceback.print_exc()
                continue

        # Second pass: infer dependencies based on tournament structure
        quarters = []
        semis = []
        finals = []
        
        # Group matches by stage
        for match in matches:
            stage = match['stage'].lower()
            if 'quarter' in stage:
                quarters.append(match)
            elif 'semi' in stage:
                semis.append(match)
            elif 'final' in stage and not 'semi' in stage:
                finals.append(match)
        
        print(f"\nFound {len(quarters)} quarter-finals, {len(semis)} semi-finals, and {len(finals)} finals")
        
        # Create dependencies between rounds
        # Quarter-finals -> Semi-finals
        for i, quarter in enumerate(quarters):
            if i < len(quarters):
                semi_idx = i // 2  # Two quarters feed into one semi
                if semi_idx < len(semis):
                    position = 1 if i % 2 == 0 else 2
                    dependencies.append({
                        'source_idx': quarter['bracket_index'],
                        'target_idx': semis[semi_idx]['bracket_index'],
                        'position': position,
                        'dependency_type': 'winner'
                    })
                    print(f"Added dependency: Quarter {i+1} -> Semi {semi_idx+1} (position {position})")
        
        # Semi-finals -> Finals
        for i, semi in enumerate(semis):
            if i < len(semis) and finals:
                position = 1 if i % 2 == 0 else 2
                dependencies.append({
                    'source_idx': semi['bracket_index'],
                    'target_idx': finals[0]['bracket_index'],
                    'position': position,
                    'dependency_type': 'winner'
                })
                print(f"Added dependency: Semi {i+1} -> Finals (position {position})")

        print(f"\nTotal matches found in this bracket: {len(matches)}")
        print(f"Total dependencies found in this bracket: {len(dependencies)}")

    return matches, dependencies

def store_team(cur, team_name, region):
    """Store or update team and return team_id"""
    if not team_name:
        return None
    cur.execute("""
        INSERT INTO teams (team_name, region)
        VALUES (%s, %s)
        ON CONFLICT (team_name) DO UPDATE 
        SET region = EXCLUDED.region
        RETURNING team_id
    """, (team_name, region))
    return cur.fetchone()[0]

def fetch_overview_page():
    url = "https://liquipedia.net/brawlstars/Brawl_Stars_Championship/2025"  # Changed to 2025
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"Successfully fetched overview page")
        return response.text
    else:
        raise Exception(f"Failed to fetch overview page. Status code: {response.status_code}")

def extract_tournament_date(link):
    # Extract month and year from the link format like:
    # /brawlstars/Brawl_Stars_Championship/2024/Season_1/EMEA/Monthly_Finals
    parts = link.split('/')
    year = int(parts[3])  # 2024
    season = int(parts[4].split('_')[1])  # 1
    
    # Map season number to month (Season 1 = March, Season 2 = April, etc)
    month_map = {
        1: 3,   # March
        2: 4,   # April
        3: 5,   # May
        4: 6,   # June
        5: 7,   # July
    }
    month = month_map.get(season, 1)  # Default to January if season not found
    
    return datetime(year, month, 1).date()

def process_tournament(conn, link, region):
    """Process a single tournament and store its matches with transaction handling"""
    tournament_name = f"{link.split('/')[-1].replace('_', ' ')} - {region}"
    tournament_date = extract_tournament_date(link)
    
    page_content = fetch_page_content(link)
    if not page_content:
        logger.error(f"Failed to fetch content for tournament: {tournament_name}")
        raise ScrapingError(f"Failed to fetch tournament content: {tournament_name}")
            
    match_soup = BeautifulSoup(page_content, 'html.parser')
    matches, dependencies = extract_match_data(match_soup, tournament_id=None)
    
    if not matches:
        logger.warning(f"No matches found for tournament: {tournament_name}")
        return
        
    all_tbd = all(
        match['team1'] is None or match['team2'] is None
        for match in matches
    )
    
    if all_tbd:
        logger.info(f"Skipping tournament {tournament_name} - all matches are TBD")
        return
    
    with conn.cursor() as cur:
        try:
            # Start transaction
            conn.autocommit = False
            
            # Store tournament
            cur.execute("""
                INSERT INTO tournaments (name, date, region)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE 
                SET date = EXCLUDED.date, region = EXCLUDED.region
                RETURNING tournament_id
            """, (tournament_name, tournament_date, region))
            tournament_id = cur.fetchone()[0]
            
            # Clear existing data within the same transaction
            cur.execute("""
                DELETE FROM match_dependencies 
                WHERE source_match_id IN (SELECT match_id FROM matches WHERE tournament_id = %s)
                OR target_match_id IN (SELECT match_id FROM matches WHERE tournament_id = %s)
            """, (tournament_id, tournament_id))
            
            cur.execute("DELETE FROM team_points WHERE tournament_id = %s", (tournament_id,))
            cur.execute("DELETE FROM matches WHERE tournament_id = %s", (tournament_id,))
            
            # Store matches and keep track of their IDs
            match_ids = {}
            matches_stored = 0
            
            for match in matches:
                if match.get('team1') is None or match.get('team2') is None:
                    continue
                    
                team1_id = store_team(cur, match['team1'], region)
                team2_id = store_team(cur, match['team2'], region)
                winner_id = None
                if match.get('winner') and match['winner'] in (match['team1'], match['team2']):
                    winner_id = store_team(cur, match['winner'], region)
                
                if team1_id is None or team2_id is None:
                    logger.warning(f"Skipping match due to invalid teams: {match['team1']} vs {match['team2']}")
                    continue
                
                cur.execute("""
                    INSERT INTO matches (
                        tournament_id, team1_id, team2_id, winner_id, 
                        score, stage, match_date, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id
                """, (
                    tournament_id, team1_id, team2_id, winner_id,
                    match.get('score'), match.get('stage'),
                    match.get('match_date'), match.get('status', 'completed')
                ))
                
                match_id = cur.fetchone()[0]
                match_ids[match['bracket_index']] = match_id
                
                if match.get('status') == 'completed' and winner_id:
                    cur.execute("""
                        INSERT INTO team_points (team_id, tournament_id, points)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (team_id, tournament_id) DO UPDATE 
                        SET points = EXCLUDED.points
                    """, (winner_id, tournament_id, 3))
                
                matches_stored += 1
            
            # Store dependencies
            deps_stored = 0
            for dep in dependencies:
                source_id = match_ids.get(dep['source_idx'])
                target_id = match_ids.get(dep['target_idx'])
                
                if source_id and target_id:
                    cur.execute("""
                        INSERT INTO match_dependencies (
                            source_match_id, target_match_id, position, dependency_type
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (source_match_id, target_match_id) DO UPDATE
                        SET position = EXCLUDED.position,
                            dependency_type = EXCLUDED.dependency_type
                    """, (source_id, target_id, dep['position'], dep['dependency_type']))
                    deps_stored += 1
            
            # Commit transaction
            conn.commit()
            logger.info(f"Successfully stored {matches_stored} matches and {deps_stored} dependencies for tournament {tournament_name}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing tournament {tournament_name}: {e}")
            raise
        finally:
            conn.autocommit = True

def display_matches_by_region(region):
    """Display all matches for a specific region"""
    try:
        conn = create_database_connection()
        with conn.cursor() as cur:
            # Get tournaments
            cur.execute("""
                SELECT name, date, region, tournament_id 
                FROM tournaments 
                WHERE region = %s
            """, (region,))
            tournaments = cur.fetchall()
            print(f"\nTournaments in {region}:")
            for t in tournaments:
                print(f"- {t[0]} (ID: {t[3]}) on {t[1]}")
            
            # Get teams
            cur.execute("""
                SELECT team_name, team_id
                FROM teams
                WHERE region = %s
            """, (region,))
            teams = cur.fetchall()
            print(f"\nTeams in {region}:")
            for t in teams:
                print(f"- {t[0]} (ID: {t[1]})")
            
            # Get matches and their dependencies
            cur.execute("""
                WITH match_info AS (
                    SELECT DISTINCT 
                        m.match_id,
                        t.name as tournament_name,
                        t1.team_name as team1,
                        t2.team_name as team2,
                        m.score,
                        m.stage,
                        m.match_date,
                        m.status
                    FROM matches m
                    JOIN tournaments t ON m.tournament_id = t.tournament_id
                    JOIN teams t1 ON m.team1_id = t1.team_id
                    JOIN teams t2 ON m.team2_id = t2.team_id
                    WHERE t.region = %s
                )
                SELECT 
                    mi.*,
                    array_agg(
                        CASE 
                            WHEN md.source_match_id IS NOT NULL 
                            THEN md.source_match_id::text || ',' || md.position::text || ',' || md.dependency_type
                            ELSE NULL 
                        END
                    ) as dependencies
                FROM match_info mi
                LEFT JOIN match_dependencies md ON mi.match_id = md.target_match_id
                GROUP BY 
                    mi.match_id, mi.tournament_name, mi.team1, mi.team2,
                    mi.score, mi.stage, mi.match_date, mi.status
                ORDER BY mi.tournament_name, mi.match_date DESC
            """, (region,))
            
            matches = cur.fetchall()
            if not matches:
                print(f"No matches found for region: {region}")
                return
            
            print(f"\nMatches in {region} region:")
            print("-" * 80)
            current_tournament = None
            
            for match in matches:
                (match_id, tournament_name, team1, team2, score, stage, 
                 match_date, status, dependencies) = match
                
                if tournament_name != current_tournament:
                    print(f"\nTournament: {tournament_name}")
                    current_tournament = tournament_name
                
                print(f"\nStage: {stage}")
                print(f"Match ID: {match_id}")
                print(f"Teams: {team1} vs {team2}")
                print(f"Score: {score}")
                print(f"Date: {match_date}")
                print(f"Status: {status}")
                
                # Show dependencies if they exist
                if dependencies and dependencies[0]:
                    for dep in dependencies:
                        if dep:
                            source_id, pos, dep_type = dep.split(',')
                            print(f"{dep_type.capitalize()} of match {source_id} advances to position {pos}")
                
                print("-" * 40)
                
    except Exception as e:
        print(f"Error displaying matches: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Main execution function with proper resource cleanup"""
    db_connection = None
    try:
        # Initialize database
        create_database()
        db_connection = create_database_connection()
        create_tables(db_connection)

        # Fetch and process overview page
        html_content = fetch_page_content("/brawlstars/Brawl_Stars_Championship/2025")
        if not html_content:
            raise ScrapingError("Failed to fetch overview page")

        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Use config for regions
        processed_links = set()  # Keep track of processed tournament links
        
        for region_name, region_filter in REGIONS.items():
            logger.info(f"\nProcessing {region_name} tournaments...")
            tournament_links = [
                a['href'] for a in soup.find_all('a', href=True)
                if 'Monthly_Finals' in a['href'] 
                and region_filter(a['href'])
                and '/2025/' in a['href']
            ]
            
            for link in tournament_links:
                if link in processed_links:  # Skip if already processed
                    continue
                    
                try:
                    process_tournament(db_connection, link, region_name)
                    processed_links.add(link)  # Mark as processed
                except (ScrapingError, DatabaseError) as e:
                    logger.error(f"Error processing tournament {link}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error processing tournament {link}: {e}")
                    continue
        
        # Display matches for each region after processing
        for region in REGIONS.keys():
            try:
                display_matches_by_region(region)
            except Exception as e:
                logger.error(f"Error displaying matches for {region}: {e}")
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, cleaning up...")
        raise
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
    finally:
        # Clean up resources
        if db_connection:
            try:
                db_connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
        
        # Close the database pool
        try:
            db_pool.close()
            logger.info("Database pool closed")
        except Exception as e:
            logger.error(f"Error closing database pool: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
        sys.exit(0)
    except Exception as e:
        logger.critical("Unhandled exception occurred", exc_info=True)
        sys.exit(1)