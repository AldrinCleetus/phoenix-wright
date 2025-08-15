#!/usr/bin/env python3
"""
Kerala High Court Case Scraper CLI
Extracts case data from court records and saves to CSV
"""

import requests
import csv
import argparse
import logging
import time
import os
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create output directory
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_DIR / 'scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SEARCH_URL = os.getenv('SEARCH_URL', 'https://hckinfo.keralacourts.in/digicourt/index.php/Casedetailssearch/Stausbycasetype')

class CourtScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_table_data(self, html_content: str, case_type: int, year: int) -> List[Dict]:
        """Extract case data from HTML table"""
        soup = BeautifulSoup(html_content, 'html.parser')
        cases = []
        
        # Find the main table
        table = soup.find('table', class_='table')
        if not table:
            logger.warning(f"No table found for case_type={case_type}, year={year}")
            return cases
        
        # Find all data rows (skip header)
        rows = table.find('tbody').find_all('tr')[:-1]  # Exclude last summary row
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                sr_no = cells[0].get_text(strip=True)
                case_no = cells[1].get_text(strip=True)
                
                # Parse petitioner vs respondent
                parties_text = cells[2].get_text(strip=True)
                if ' v/s ' in parties_text:
                    petitioner, respondent = parties_text.split(' v/s ', 1)
                    petitioner = petitioner.strip()
                    respondent = respondent.strip()
                else:
                    petitioner = parties_text
                    respondent = ""
                
                # Extract onclick parameters (CINO and case number)
                cino = ""
                case_number = ""
                if len(cells) >= 4:
                    button = cells[3].find('button')
                    if button and button.get('onclick'):
                        onclick_str = button.get('onclick')
                        # Extract parameters from ViewCaseStatus('PARAM1','PARAM2')
                        if "ViewCaseStatus('" in onclick_str:
                            params_part = onclick_str.split("ViewCaseStatus('")[1].split("');")[0]
                            params = params_part.split("','")
                            if len(params) >= 2:
                                cino = params[0]
                                case_number = params[1]
                
                cases.append({
                    'case_type': case_type,
                    'year': year,
                    'sr_no': sr_no,
                    'case_no': case_no,
                    'petitioner': petitioner,
                    'respondent': respondent,
                    'cino': cino,
                    'case_number': case_number
                })
        
        return cases
    
    def fetch_cases(self, case_type: int, year: int) -> List[Dict]:
        """Fetch cases for given case type and year"""
        payload = {
            'case_type': case_type,
            'case_year': year
        }
        
        try:
            logger.info(f"Fetching case_type={case_type}, year={year}")
            response = self.session.post(SEARCH_URL, data=payload, timeout=30)
            response.raise_for_status()
            
            cases = self.extract_table_data(response.text, case_type, year)
            logger.info(f"Found {len(cases)} cases for case_type={case_type}, year={year}")
            
            return cases
            
        except requests.RequestException as e:
            logger.error(f"Request failed for case_type={case_type}, year={year}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error processing case_type={case_type}, year={year}: {e}")
            return []
    
    def save_to_csv(self, cases: List[Dict], filename: str):
        """Save cases to CSV file in output directory"""
        if not cases:
            logger.warning("No cases to save")
            return
        
        # Ensure filename is in output directory
        filepath = OUTPUT_DIR / filename
        
        fieldnames = ['case_type', 'year', 'sr_no', 'case_no', 'petitioner', 'respondent', 'cino', 'case_number']
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cases)
        
        logger.info(f"Saved {len(cases)} cases to {filepath}")

def main():
    parser = argparse.ArgumentParser(description='Kerala High Court Case Scraper')
    parser.add_argument('--case-type', type=int, help='Case type (1-260). If not specified, all case types will be processed')
    parser.add_argument('--year', type=int, help='Year to search')
    parser.add_argument('--year-start', type=int, help='Start year for range')
    parser.add_argument('--year-end', type=int, help='End year for range')
    parser.add_argument('--output', default='cases.csv', help='Output CSV filename')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests (seconds)')
    
    args = parser.parse_args()
    
    scraper = CourtScraper()
    all_cases = []
    
    # Determine case types to process
    if args.case_type:
        case_types = [args.case_type]
    else:
        case_types = range(1, 261)
        logger.info("No case type specified, processing all case types (1-260)")
    
    # Determine years to process
    if args.year_start and args.year_end:
        years = range(args.year_start, args.year_end + 1)
    elif args.year:
        years = [args.year]
    else:
        logger.error("Must specify --year or --year-start/--year-end")
        return
    
    # Process all combinations
    total_requests = len(case_types) * len(years)
    current_request = 0
    
    logger.info(f"Starting scrape: {len(case_types)} case types x {len(years)} years = {total_requests} requests")
    
    for case_type in case_types:
        for year in years:
            current_request += 1
            logger.info(f"Progress: {current_request}/{total_requests}")
            
            cases = scraper.fetch_cases(case_type, year)
            all_cases.extend(cases)
            
            # Rate limiting
            if current_request < total_requests:
                time.sleep(args.delay)
    
    # Save results
    scraper.save_to_csv(all_cases, args.output)
    logger.info(f"Scraping complete. Total cases: {len(all_cases)}")

if __name__ == '__main__':
    main()