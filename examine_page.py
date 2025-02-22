import requests
from bs4 import BeautifulSoup
import time

def fetch_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    time.sleep(2)  # Be nice to the server
    response = requests.get(url, headers=headers)
    return response.text

def examine_page(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    main_content = soup.find('div', class_='mw-parser-output')
    
    if not main_content:
        print("Could not find main content area")
        return

    # Look at all divs with classes
    print("\n=== All div elements with classes ===")
    divs = main_content.find_all('div', class_=True)
    for div in divs:
        classes = div.get('class')
        if classes:
            print(f"\nClasses: {' '.join(classes)}")
            # Print a snippet of content to help identify what this div contains
            content = div.get_text(strip=True)[:100]
            print(f"Content preview: {content}")

    # Look at all tables
    print("\n=== All tables with classes ===")
    tables = main_content.find_all('table', class_=True)
    for table in tables:
        classes = table.get('class')
        if classes:
            print(f"\nTable classes: {' '.join(classes)}")
            # Print first row to see structure
            first_row = table.find('tr')
            if first_row:
                headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]
                print(f"Headers/First row: {headers}")

    # Look specifically for elements that might contain match data
    print("\n=== Potential match elements ===")
    match_elements = main_content.find_all(['div', 'table'], 
        class_=lambda x: x and any(term in str(x).lower() for term in ['match', 'game', 'bracket', 'vs', 'versus']))
    
    for elem in match_elements:
        print(f"\nElement type: {elem.name}")
        print(f"Classes: {elem.get('class')}")
        content = elem.get_text(strip=True)[:200]
        print(f"Content preview: {content}")

def main():
    # Example URL of a tournament page
    url = "https://liquipedia.net/brawlstars/Brawl_Stars_Championship/2024/Season_1/EMEA/Monthly_Finals"
    html_content = fetch_page(url)
    examine_page(html_content)

if __name__ == "__main__":
    main()

"""Page parsing and examination utilities"""
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Tuple, List, Dict, Optional
import logging
from schema import Match, Tournament, Team, MatchDependency
from exceptions import ScrapingError, DataValidationError
from config import REQUEST_TIMEOUT
import requests
import time

logger = logging.getLogger(__name__)

class PageAnalyzer:
    """Analyzes HTML page structure and content"""
    
    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.main_content = self.soup.find('div', class_='mw-parser-output')

    def analyze_structure(self) -> Dict:
        """Analyze page structure and return statistics"""
        if not self.main_content:
            raise ScrapingError("Could not find main content area")

        stats = {
            'tables': len(self.main_content.find_all('table')),
            'brackets': len(self.main_content.find_all('div', class_=lambda x: x and 'bracket' in x)),
            'match_elements': len(self.main_content.find_all('div', class_=lambda x: x and 'match' in x)),
            'team_elements': len(self.main_content.find_all('div', class_=lambda x: x and 'team' in x))
        }
        
        logger.info(f"Page structure analysis: {stats}")
        return stats

    def find_match_elements(self) -> List[Dict]:
        """Find and analyze all match-related elements"""
        matches = []
        match_elements = self.main_content.find_all('div', 
            class_=lambda x: x and any(term in str(x).lower() for term in ['match', 'game', 'vs']))
        
        for elem in match_elements:
            match_info = {
                'type': elem.get('class', []),
                'content': elem.get_text(strip=True)[:100],
                'has_score': bool(elem.find(class_=lambda x: x and 'score' in str(x).lower())),
                'has_date': bool(elem.find(class_=lambda x: x and 'date' in str(x).lower()))
            }
            matches.append(match_info)
        
        return matches

    def analyze_bracket_structure(self) -> Dict:
        """Analyze tournament bracket structure"""
        brackets = self.main_content.find_all('div', class_=lambda x: x and 'bracket' in x)
        structure = {
            'total_brackets': len(brackets),
            'rounds': [],
            'hierarchy': {}
        }
        
        for bracket in brackets:
            rounds = bracket.find_all('div', class_='brkts-round')
            structure['rounds'].append(len(rounds))
            
            # Analyze bracket hierarchy
            for i, round_elem in enumerate(rounds):
                matches = round_elem.find_all('div', class_='brkts-match')
                structure['hierarchy'][f'round_{i+1}'] = len(matches)
        
        return structure

def extract_tournament_info(soup: BeautifulSoup, region: str) -> Tournament:
    """Extract tournament information from the page"""
    try:
        title = soup.find('h1', class_='firstHeading').get_text(strip=True)
        date_elem = soup.find('div', class_='date-container')
        date = datetime.now()  # fallback
        
        if date_elem:
            try:
                date = datetime.strptime(date_elem.get_text(strip=True), '%Y-%m-%d')
            except ValueError:
                logger.warning(f"Could not parse date from {date_elem.get_text(strip=True)}")
        
        return Tournament(
            tournament_id=None,
            name=title,
            date=date,
            region=region
        )
    except Exception as e:
        raise ScrapingError(f"Failed to extract tournament info: {e}")

def validate_bracket_structure(matches: List[Match], dependencies: List[MatchDependency]) -> bool:
    """Validate the overall bracket structure"""
    try:
        # Ensure no circular dependencies
        match_ids = {m.match_id for m in matches if m.match_id is not None}
        for dep in dependencies:
            if dep.source_match_id not in match_ids or dep.target_match_id not in match_ids:
                return False
            
        # Validate each match and dependency
        for match in matches:
            if not match.validate():
                return False
                
        for dep in dependencies:
            if not dep.validate():
                return False
                
        return True
    except Exception as e:
        raise DataValidationError(f"Bracket validation failed: {e}")

def analyze_team_performance(matches: List[Match]) -> Dict[int, Dict]:
    """Analyze team performance statistics"""
    stats = {}
    for match in matches:
        for team_id in [match.team1_id, match.team2_id]:
            if team_id not in stats:
                stats[team_id] = {'wins': 0, 'matches': 0}
            stats[team_id]['matches'] += 1
            if match.winner_id == team_id:
                stats[team_id]['wins'] += 1
    return stats