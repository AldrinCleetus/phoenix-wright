#!/usr/bin/env python3
"""
Data Verification and Gap Filling Tool
Verifies completeness of scraped data and fills missing CINOs
"""

import json
import csv
import logging
import asyncio
import aiohttp
from pathlib import Path
from typing import Set, List, Dict, Optional
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_DIR / 'verification.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataVerifier:
    def __init__(self):
        self.urls = {
            'interim_orders': os.getenv('INTERIM_ORDERS_URL'),
            'acts_data': os.getenv('ACTS_DATA_URL'),
            'cnr_data': os.getenv('CNR_DATA_URL'),
            'case_history': os.getenv('CASE_HISTORY_URL'),
            'judgments': os.getenv('JUDGMENTS_URL'),
            'documents': os.getenv('DOCUMENTS_URL'),
            'connected_cases': os.getenv('CONNECTED_CASES_URL'),
        }
        self.base_url = os.getenv('DETAILS_BASE_URL', 'https://hckdemo.keralacourts.in')
    
    def load_csv_cinos(self, csv_filename: str) -> Dict[str, Dict]:
        """Load all CINOs from original CSV with their metadata"""
        csv_path = OUTPUT_DIR / csv_filename
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return {}
        
        cino_data = {}
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                cino = row.get('cino', '').strip()
                if cino:
                    cino_data[cino] = {
                        'case_type': int(row.get('case_type', 0)),
                        'year': int(row.get('year', 0)),
                        'case_no': row.get('case_no', ''),
                        'petitioner': row.get('petitioner', ''),
                        'respondent': row.get('respondent', '')
                    }
        
        logger.info(f"Loaded {len(cino_data)} CINOs from CSV")
        return cino_data
    
    def load_json_cinos(self, json_filename: str) -> Set[str]:
        """Load all CINOs from scraped JSON data"""
        json_path = OUTPUT_DIR / json_filename
        
        if not json_path.exists():
            logger.warning(f"JSON file not found: {json_path}")
            return set()
        
        try:
            with open(json_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
            
            if isinstance(data, list):
                scraped_cinos = {item.get('cino', '') for item in data if item.get('cino')}
            else:
                logger.error("JSON data is not in expected list format")
                return set()
            
            logger.info(f"Found {len(scraped_cinos)} CINOs in JSON data")
            return scraped_cinos
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON file: {e}")
            return set()
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}")
            return set()
    
    def find_missing_cinos(self, csv_filename: str, json_filename: str) -> Dict[str, Dict]:
        """Find CINOs that are in CSV but not in JSON"""
        csv_cinos = self.load_csv_cinos(csv_filename)
        json_cinos = self.load_json_cinos(json_filename)
        
        missing_cinos = {}
        for cino, metadata in csv_cinos.items():
            if cino not in json_cinos:
                missing_cinos[cino] = metadata
        
        logger.info(f"Found {len(missing_cinos)} missing CINOs")
        return missing_cinos
    
    def analyze_data_quality(self, json_filename: str) -> Dict:
        """Analyze the quality and completeness of scraped data"""
        json_path = OUTPUT_DIR / json_filename
        
        if not json_path.exists():
            logger.error(f"JSON file not found: {json_path}")
            return {}
        
        try:
            with open(json_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}")
            return {}
        
        if not isinstance(data, list):
            logger.error("JSON data is not in expected list format")
            return {}
        
        analysis = {
            'total_cases': len(data),
            'empty_cases': 0,
            'missing_fields': {
                'interim_orders': 0,
                'acts_data': 0,
                'cnr_data': 0,
                'case_history': 0,
                'judgments': 0,
                'documents': 0,
                'connected_cases': 0
            },
            'cases_with_data': {
                'interim_orders': 0,
                'acts_data': 0,
                'cnr_data': 0,
                'case_history': 0,
                'judgments': 0,
                'documents': 0,
                'connected_cases': 0
            }
        }
        
        for case in data:
            if not case or not case.get('cino'):
                analysis['empty_cases'] += 1
                continue
            
            for field in analysis['missing_fields'].keys():
                if field not in case or not case[field]:
                    analysis['missing_fields'][field] += 1
                else:
                    analysis['cases_with_data'][field] += 1
        
        return analysis
    
    def print_analysis_report(self, analysis: Dict):
        """Print a detailed analysis report"""
        if not analysis:
            logger.error("No analysis data available")
            return
        
        print("\n" + "="*60)
        print("DATA QUALITY ANALYSIS REPORT")
        print("="*60)
        
        print(f"\nTotal Cases: {analysis['total_cases']}")
        print(f"Empty Cases: {analysis['empty_cases']}")
        print(f"Valid Cases: {analysis['total_cases'] - analysis['empty_cases']}")
        
        print(f"\nDATA AVAILABILITY:")
        print("-" * 40)
        for field, missing_count in analysis['missing_fields'].items():
            available_count = analysis['cases_with_data'][field]
            total_valid = analysis['total_cases'] - analysis['empty_cases']
            
            if total_valid > 0:
                availability_pct = (available_count / total_valid) * 100
                print(f"{field:18} : {available_count:4d}/{total_valid:4d} ({availability_pct:5.1f}%)")
        
        print("\n" + "="*60)
    
    def save_missing_cinos_csv(self, missing_cinos: Dict[str, Dict], filename: str = "missing_cinos.csv"):
        """Save missing CINOs to a CSV file for manual review"""
        if not missing_cinos:
            logger.info("No missing CINOs to save")
            return
        
        filepath = OUTPUT_DIR / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['cino', 'case_type', 'year', 'case_no', 'petitioner', 'respondent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for cino, metadata in missing_cinos.items():
                row = {'cino': cino, **metadata}
                writer.writerow(row)
        
        logger.info(f"Saved {len(missing_cinos)} missing CINOs to {filepath}")

class MissingDataScraper:
    """Scraper specifically for filling in missing CINOs"""
    
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.urls = {
            'interim_orders': os.getenv('INTERIM_ORDERS_URL'),
            'acts_data': os.getenv('ACTS_DATA_URL'),
            'cnr_data': os.getenv('CNR_DATA_URL'),
            'case_history': os.getenv('CASE_HISTORY_URL'),
            'judgments': os.getenv('JUDGMENTS_URL'),
            'documents': os.getenv('DOCUMENTS_URL'),
            'connected_cases': os.getenv('CONNECTED_CASES_URL'),
        }
        self.base_url = os.getenv('DETAILS_BASE_URL', 'https://hckdemo.keralacourts.in')
    
    async def scrape_missing_cinos(self, missing_cinos: Dict[str, Dict]) -> List[Dict]:
        """Scrape data for missing CINOs"""
        if not missing_cinos:
            logger.info("No missing CINOs to scrape")
            return []
        
        logger.info(f"Scraping {len(missing_cinos)} missing CINOs")
        
        # Import the scraper class from the main scraper
        from case_details import AsyncCaseDetailsScraper
        
        scraper = AsyncCaseDetailsScraper(max_concurrent=self.max_concurrent)
        
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,
            limit_per_host=self.max_concurrent
        )
        
        results = []
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Content-Type': 'application/json'
            }
        ) as session:
            
            cino_list = list(missing_cinos.keys())
            
            # Process in small batches
            for i in range(0, len(cino_list), self.max_concurrent):
                batch_cinos = cino_list[i:i + self.max_concurrent]
                batch_num = i // self.max_concurrent + 1
                total_batches = (len(cino_list) + self.max_concurrent - 1) // self.max_concurrent
                
                logger.info(f"Processing missing CINOs batch {batch_num}/{total_batches}")
                
                tasks = []
                for cino in batch_cinos:
                    metadata = missing_cinos[cino]
                    task = scraper.fetch_all_endpoints_for_case(session, cino)
                    tasks.append((cino, metadata, task))
                
                # Execute batch
                for cino, metadata, task in tasks:
                    try:
                        result = await task
                        if result:
                            result.update({
                                'case_type': metadata['case_type'],
                                'year': metadata['year']
                            })
                            results.append(result)
                            logger.info(f"Successfully scraped missing CINO: {cino}")
                    except Exception as e:
                        logger.error(f"Failed to scrape CINO {cino}: {e}")
                
                # Small delay between batches
                if i + self.max_concurrent < len(cino_list):
                    await asyncio.sleep(2.0)
        
        logger.info(f"Successfully scraped {len(results)} missing CINOs")
        return results

def merge_json_files(original_file: str, missing_data_file: str, output_file: str):
    """Merge original JSON data with newly scraped missing data"""
    original_path = OUTPUT_DIR / original_file
    missing_path = OUTPUT_DIR / missing_data_file
    output_path = OUTPUT_DIR / output_file
    
    # Load original data
    original_data = []
    if original_path.exists():
        try:
            with open(original_path, 'r', encoding='utf-8') as f:
                original_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading original data: {e}")
            return
    
    # Load missing data
    missing_data = []
    if missing_path.exists():
        try:
            with open(missing_path, 'r', encoding='utf-8') as f:
                missing_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading missing data: {e}")
            return
    
    # Merge data
    merged_data = original_data + missing_data
    
    # Save merged data
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Merged data saved to {output_path}")
    logger.info(f"Total records: {len(merged_data)} (original: {len(original_data)}, missing: {len(missing_data)})")

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Verification and Gap Filling Tool')
    parser.add_argument('--input-csv', required=True, help='Original CSV filename')
    parser.add_argument('--input-json', required=True, help='Scraped JSON filename')
    parser.add_argument('--action', choices=['verify', 'scrape-missing', 'analyze'], 
                       default='verify', help='Action to perform')
    parser.add_argument('--output-missing', default='missing_data.json', 
                       help='Output file for missing data')
    parser.add_argument('--output-merged', default='complete_data.json', 
                       help='Output file for merged complete data')
    parser.add_argument('--max-concurrent', type=int, default=5, 
                       help='Max concurrent requests for missing data scraping')
    
    args = parser.parse_args()
    
    verifier = DataVerifier()
    
    if args.action == 'verify':
        # Find missing CINOs
        missing_cinos = verifier.find_missing_cinos(args.input_csv, args.input_json)
        
        if missing_cinos:
            print(f"\n⚠️  MISSING DATA DETECTED!")
            print(f"Found {len(missing_cinos)} CINOs in CSV that are not in JSON")
            print(f"This represents missing data that needs to be scraped.")
            
            # Save missing CINOs for review
            verifier.save_missing_cinos_csv(missing_cinos)
            
            print(f"\nTo scrape missing data, run:")
            print(f"python verify_data.py --input-csv {args.input_csv} --input-json {args.input_json} --action scrape-missing")
        else:
            print(f"\n✅ ALL DATA COMPLETE!")
            print(f"All CINOs from CSV are present in JSON data.")
    
    elif args.action == 'analyze':
        # Analyze data quality
        analysis = verifier.analyze_data_quality(args.input_json)
        verifier.print_analysis_report(analysis)
    
    elif args.action == 'scrape-missing':
        # Find and scrape missing CINOs
        missing_cinos = verifier.find_missing_cinos(args.input_csv, args.input_json)
        
        if not missing_cinos:
            print("✅ No missing CINOs found!")
            return
        
        print(f"🔄 Scraping {len(missing_cinos)} missing CINOs...")
        
        scraper = MissingDataScraper(max_concurrent=args.max_concurrent)
        missing_data = await scraper.scrape_missing_cinos(missing_cinos)
        
        if missing_data:
            # Save missing data
            missing_path = OUTPUT_DIR / args.output_missing
            with open(missing_path, 'w', encoding='utf-8') as f:
                json.dump(missing_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Missing data saved to {missing_path}")
            
            # Merge with original data
            merge_json_files(args.input_json, args.output_missing, args.output_merged)
            
            print(f"\n✅ MISSING DATA SCRAPED!")
            print(f"Complete dataset saved to: {args.output_merged}")
        else:
            print("❌ No missing data could be scraped")

if __name__ == '__main__':
    asyncio.run(main())