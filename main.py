import os
import time
import re
from bs4 import BeautifulSoup
import requests
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
from contextlib import contextmanager

# Load environment variables
load_dotenv()

@contextmanager
def get_db_connection(dbname='postgres'):
    """Context manager for database connections"""
    conn = psycopg2.connect(
        dbname=dbname,
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()

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

def create_database_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def create_tables(conn):
    with conn.cursor() as cur:
        # Create teams table with increased field lengths
        cur.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id SERIAL PRIMARY KEY,
                team_name VARCHAR(200) UNIQUE NOT NULL,
                region VARCHAR(100) NOT NULL
            )
        """)
        
        # Create tournaments table with increased field lengths
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tournaments (
                tournament_id SERIAL PRIMARY KEY,
                name VARCHAR(300) UNIQUE NOT NULL,
                date DATE NOT NULL,
                region VARCHAR(100) NOT NULL
            )
        """)
        
        # Create matches table with increased field lengths
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id SERIAL PRIMARY KEY,
                tournament_id INTEGER REFERENCES tournaments(tournament_id),
                team1_id INTEGER REFERENCES teams(team_id),
                team2_id INTEGER REFERENCES teams(team_id),
                winner_id INTEGER REFERENCES teams(team_id),
                score VARCHAR(50),
                stage VARCHAR(200),
                match_date TIMESTAMP,
                status VARCHAR(50) DEFAULT 'completed',
                UNIQUE(tournament_id, team1_id, team2_id, match_date)
            )
        """)
        
        # Create team points table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS team_points (
                id SERIAL PRIMARY KEY,
                team_id INTEGER REFERENCES teams(team_id),
                tournament_id INTEGER REFERENCES tournaments(tournament_id),
                points INTEGER NOT NULL,
                UNIQUE(team_id, tournament_id)
            )
        """)
        
        # Create match_dependencies table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS match_dependencies (
                dependency_id SERIAL PRIMARY KEY,
                source_match_id INTEGER REFERENCES matches(match_id),
                target_match_id INTEGER REFERENCES matches(match_id),
                position INTEGER CHECK (position IN (1, 2)),
                dependency_type VARCHAR(50) CHECK (dependency_type IN ('winner', 'loser')),
                UNIQUE(source_match_id, target_match_id)
            )
        """)
        
        conn.commit()

def fetch_page_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    time.sleep(2)  # Rate limiting
    full_url = f"https://liquipedia.net{url}"
    print(f"\nFetching {full_url}")
    
    try:
        response = requests.get(full_url, headers=headers)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            content = response.text
            print(f"Content length: {len(content)} bytes")
            
            soup = BeautifulSoup(content, 'html.parser')
            main_content = soup.find('div', class_='mw-parser-output')
            
            if (main_content):
                print("\nFound main content area:")
                print(f"Tables found: {len(main_content.find_all('table'))}")
                print(f"Bracket elements: {len(main_content.find_all('div', class_=lambda x: x and 'bracket' in x))}")
            
            return content
        else:
            print(f"Failed to fetch page. Status code: {response.status_code}")
            return None
            
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None

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
    """Clean team names by removing team tags"""
    if not raw_name or 'TBD' in raw_name:
        return None
        
    # Common team name patterns to clean up
    patterns = [
        r'(.*?)(Gaming|Esports?|NA|TRB|NASKC|PSM|TE|TH|KZK|ECP|FUT|SK|OG)\s*$',  # Suffixes
        r'(.*?)\s+[A-Z]+\s*$'  # Capital letter codes at the end
    ]
    
    name = raw_name.strip()
    for pattern in patterns:
        match = re.search(pattern, name)
        if match:
            name = match.group(1).strip()
            
    return name if name else raw_name

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
    """Process a single tournament and store its matches"""
    tournament_name = f"{link.split('/')[-1].replace('_', ' ')} - {region}"
    tournament_date = extract_tournament_date(link)
    
    page_content = fetch_page_content(link)
    if not page_content:
        print(f"Failed to fetch content for tournament: {tournament_name}")
        return
            
    match_soup = BeautifulSoup(page_content, 'html.parser')
    matches, dependencies = extract_match_data(match_soup, tournament_id=None)
    
    if not matches:
        print(f"No matches found for tournament: {tournament_name}")
        return
        
    all_tbd = all(
        match['team1'] is None or match['team2'] is None
        for match in matches
    )
    
    if all_tbd:
        print(f"Skipping tournament {tournament_name} - all matches are TBD")
        return
    
    with conn.cursor() as cur:
        # Store tournament
        cur.execute("""
            INSERT INTO tournaments (name, date, region)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE 
            SET date = EXCLUDED.date, region = EXCLUDED.region
            RETURNING tournament_id
        """, (tournament_name, tournament_date, region))
        tournament_id = cur.fetchone()[0]
        
        # Clear existing data
        cur.execute("""
            DELETE FROM match_dependencies 
            WHERE source_match_id IN (SELECT match_id FROM matches WHERE tournament_id = %s)
            OR target_match_id IN (SELECT match_id FROM matches WHERE tournament_id = %s);
            DELETE FROM team_points WHERE tournament_id = %s;
            DELETE FROM matches WHERE tournament_id = %s;
        """, (tournament_id, tournament_id, tournament_id, tournament_id))
        
        # Store matches first and keep track of their IDs
        match_ids = {}  # Maps bracket_index to match_id
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
                print(f"Skipping match due to invalid teams: {match['team1']} vs {match['team2']}")
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
        
        # Store dependencies after all matches are stored
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
        
        conn.commit()
        print(f"Stored {matches_stored} matches and {deps_stored} dependencies for tournament {tournament_name}")

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
    try:
        # Initialize database
        create_database()
        conn = create_database_connection()
        create_tables(conn)

        # Fetch and process overview page
        html_content = fetch_overview_page()
        if not html_content:
            raise Exception("Failed to fetch overview page")

        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Process each region's tournaments
        regions = {
            'EMEA': lambda x: 'EMEA' in x,
            'North_America': lambda x: 'North_America' in x
        }
        
        processed_links = set()  # Keep track of processed tournament links
        
        for region_name, region_filter in regions.items():
            print(f"\nProcessing {region_name} tournaments...")
            tournament_links = [
                a['href'] for a in soup.find_all('a', href=True)
                if 'Monthly_Finals' in a['href'] 
                and region_filter(a['href'])
                and '/2025/' in a['href']  # Only process 2025 tournaments
            ]
            
            for link in tournament_links:
                if link in processed_links:  # Skip if already processed
                    continue
                    
                try:
                    process_tournament(conn, link, region_name)
                    processed_links.add(link)  # Mark as processed
                except Exception as e:
                    print(f"Error processing tournament {link}: {e}")
                    continue
        
        # Display matches for each region after processing
        for region in regions.keys():
            display_matches_by_region(region)
            
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()