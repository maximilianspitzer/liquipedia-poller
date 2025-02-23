import os
import sys
import time
import json
import signal
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import threading
from main import create_database_connection, process_tournament, fetch_page_content
from migrations import run_migrations
from bs4 import BeautifulSoup
from config import *
from exceptions import ScrapingError, DatabaseError
import psycopg2

class LiquipediaPollerService:
    def __init__(self):
        self.setup_logging()
        self.running = False
        self.last_run = None
        self.current_status = {"status": "stopped", "last_run": None, "last_error": None}
        self.status_lock = threading.Lock()
        self.health_check_interval = 300  # 5 minutes
        
    def initialize_database(self):
        """Initialize database with retries"""
        max_retries = 5
        retry_delay = 10
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempting database initialization (attempt {attempt + 1}/{max_retries})")
                conn = create_database_connection()
                
                # Run migrations
                self.logger.info("Running database migrations")
                run_migrations(conn)
                
                conn.close()
                self.logger.info("Database initialization successful")
                return True
                
            except Exception as e:
                self.logger.error(f"Database initialization attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Database initialization failed after all retries")
                    raise

    def check_database_health(self):
        """Check if database connection is healthy"""
        try:
            conn = create_database_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False

    def health_check_loop(self):
        """Continuous health check loop"""
        while self.running:
            try:
                if not self.check_database_health():
                    self.logger.error("Database health check failed, attempting to reinitialize")
                    try:
                        self.initialize_database()
                    except Exception as e:
                        self.logger.error(f"Database reinitialization failed: {e}")
                time.sleep(self.health_check_interval)
            except Exception as e:
                self.logger.error(f"Error in health check loop: {e}")
                time.sleep(self.health_check_interval)
        
    def setup_logging(self):
        """Configure logging with rotation"""
        self.logger = logging.getLogger("LiquipediaPoller")
        self.logger.setLevel(getattr(logging, LOG_LEVEL))
        
        formatter = logging.Formatter(LOG_FORMAT)
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_SIZE,
            backupCount=LOG_BACKUP_COUNT
        )
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def update_status(self, status=None, error=None):
        """Update service status and save to file"""
        with self.status_lock:
            if status:
                self.current_status["status"] = status
            if error:
                self.current_status["last_error"] = str(error)
            self.current_status["last_run"] = datetime.now().isoformat()
            
            try:
                with open(STATUS_FILE, 'w') as f:
                    json.dump(self.current_status, f, indent=2)
            except Exception as e:
                self.logger.error(f"Failed to save status file: {e}")

    def process_all_tournaments(self):
        """Process all tournaments for all regions"""
        try:
            self.logger.info("Starting tournament processing cycle")
            self.update_status(status="running")
            
            db_connection = create_database_connection()
            
            html_content = fetch_page_content("/brawlstars/Brawl_Stars_Championship/2025")
            if not html_content:
                raise ScrapingError("Failed to fetch overview page")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            processed_links = set()
            
            for region_name, region_filter in REGIONS.items():
                self.logger.info(f"\nProcessing {region_name} tournaments...")
                tournament_links = sorted(
                    [a['href'] for a in soup.find_all('a', href=True)
                    if 'Monthly_Finals' in a['href'] 
                    and region_filter(a['href'])
                    and '/2025/' in a['href']],
                    key=lambda x: int(x.split('Season_')[1].split('/')[0])
                )
                
                self.logger.info(f"Found {len(tournament_links)} tournaments for {region_name}")
                
                for i, link in enumerate(tournament_links):
                    if link in processed_links:
                        self.logger.info(f"Skipping already processed tournament: {link}")
                        continue
                        
                    try:
                        season = int(link.split('Season_')[1].split('/')[0])
                        self.logger.info(f"Processing Season {season} for {region_name}")
                        result = process_tournament(db_connection, link, region_name)
                        
                        if result is False:
                            remaining = len(tournament_links) - i - 1
                            self.logger.info(f"Season {season} has all TBD matches. Skipping {remaining} remaining seasons for {region_name}")
                            # Add remaining seasons to processed_links to skip them
                            for future_link in tournament_links[i+1:]:
                                future_season = int(future_link.split('Season_')[1].split('/')[0])
                                self.logger.info(f"  - Skipping {region_name} Season {future_season}")
                                processed_links.add(future_link)
                            break
                            
                        processed_links.add(link)
                    except (ScrapingError, DatabaseError) as e:
                        self.logger.error(f"Error processing tournament {link}: {e}")
                        continue
                    except Exception as e:
                        self.logger.error(f"Unexpected error processing tournament {link}: {e}")
                        continue
            
            self.last_run = datetime.now()
            self.update_status(status="idle")
            self.logger.info("Tournament processing cycle completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in processing cycle: {e}")
            self.update_status(status="error", error=str(e))
            raise
        finally:
            if 'db_connection' in locals():
                db_connection.close()

    def run(self):
        """Main service loop"""
        self.running = True
        self.logger.info("Starting Liquipedia Poller Service")
        
        try:
            # Initialize database first
            self.initialize_database()
            
            # Start health check in a separate thread
            health_thread = threading.Thread(target=self.health_check_loop, daemon=True)
            health_thread.start()
            
            def signal_handler(signum, frame):
                self.logger.info(f"Received signal {signum}")
                self.stop()
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            while self.running:
                try:
                    self.process_all_tournaments()
                    self.logger.info(f"Sleeping for {UPDATE_INTERVAL} seconds")
                    time.sleep(UPDATE_INTERVAL)
                except Exception as e:
                    self.logger.error(f"Error in service loop: {e}")
                    time.sleep(RETRY_DELAY)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in service: {e}")
            raise
        finally:
            self.running = False
            self.update_status(status="stopped")

    def stop(self):
        """Stop the service"""
        self.logger.info("Stopping service...")
        self.running = False
        self.update_status(status="stopped")

def run_service():
    """Run the service"""
    service = LiquipediaPollerService()
    try:
        service.run()
    except KeyboardInterrupt:
        service.logger.info("Service interrupted by user")
    except Exception as e:
        service.logger.error(f"Service crashed: {e}")
        raise
    finally:
        service.stop()

if __name__ == "__main__":
    run_service()