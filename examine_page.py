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