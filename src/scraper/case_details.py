#!/usr/bin/env python3
"""
Async Kerala High Court Case Details Scraper
High-performance version with concurrent processing
"""

import asyncio
import aiohttp
import json
import csv
import logging
import time
import base64
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Set
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

class AsyncCaseDetailsScraper:
    def __init__(self, max_concurrent: int = 10, delay_between_batches: float = 1.0):
        self.max_concurrent = max_concurrent
        self.delay_between_batches = delay_between_batches
        self.retry_delay = 60.0  # 1 minute retry delay
        self.max_retries = 3
        
        # Load URLs from environment
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
        
        # Validate URLs
        missing_urls = [name for name, url in self.urls.items() if not url]
        if missing_urls:
            logger.warning(f"Missing URLs in .env: {missing_urls}")
    
    def get_progress_file_path(self, base_filename: str) -> Path:
        """Get path for progress tracking file"""
        name = Path(base_filename).stem
        return OUTPUT_DIR / f"{name}_progress.json"
    
    def get_temp_results_file_path(self, base_filename: str) -> Path:
        """Get path for temporary results file"""
        name = Path(base_filename).stem
        return OUTPUT_DIR / f"{name}_temp_results.json"
    
    def load_progress(self, base_filename: str) -> Dict:
        """Load progress from file"""
        progress_file = self.get_progress_file_path(base_filename)
        
        if progress_file.exists():
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        
        return {
            'processed_cinos': [],
            'current_batch': 0,
            'total_batches': 0,
            'completed': False
        }
    
    def save_progress(self, base_filename: str, processed_cinos: List[str], 
                     current_batch: int, total_batches: int, completed: bool = False):
        """Save current progress"""
        progress_file = self.get_progress_file_path(base_filename)
        
        progress_data = {
            'processed_cinos': processed_cinos,
            'current_batch': current_batch,
            'total_batches': total_batches,
            'completed': completed,
            'last_updated': time.time()
        }
        
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, indent=2)
        
        logger.info(f"Progress saved: batch {current_batch}/{total_batches}")
    
    def load_existing_results(self, base_filename: str) -> List[Dict]:
        """Load existing results from temporary file"""
        temp_file = self.get_temp_results_file_path(base_filename)
        
        if temp_file.exists():
            try:
                with open(temp_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load existing results: {e}")
        
        return []
    
    def save_batch_results(self, base_filename: str, new_results: List[Dict], append: bool = True):
        """Save batch results to temporary file"""
        temp_file = self.get_temp_results_file_path(base_filename)
        
        if append and temp_file.exists():
            # Load existing results and append
            existing_results = self.load_existing_results(base_filename)
            all_results = existing_results + new_results
        else:
            all_results = new_results
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Batch results saved: {len(new_results)} new cases, {len(all_results)} total")
    
    async def check_network_connectivity(self) -> bool:
        """Check if network is available"""
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.base_url) as response:
                    return response.status < 500
        except Exception:
            return False
    
    async def wait_for_network_recovery(self):
        """Wait for network to recover with periodic checks"""
        logger.warning("Network connectivity issues detected. Waiting for recovery...")
        
        retry_count = 0
        while True:
            retry_count += 1
            logger.info(f"Network check attempt {retry_count}")
            
            if await self.check_network_connectivity():
                logger.info("Network connectivity restored!")
                break
            
            logger.info(f"Network still unavailable. Retrying in {self.retry_delay}s...")
            await asyncio.sleep(self.retry_delay)
    
    async def _make_request(self, session: aiohttp.ClientSession, method: str, url: str, **kwargs) -> Optional[Dict]:
        """Make async HTTP request with error handling and network recovery"""
        for attempt in range(self.max_retries):
            try:
                async with session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Request failed after {self.max_retries} attempts for {url}: {e}")
                    return None
                
                # Check if it's a network connectivity issue
                if not await self.check_network_connectivity():
                    await self.wait_for_network_recovery()
                
                # Wait before retry
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}), retrying in {wait_time}s")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"Unexpected error for {url}: {e}")
                return None
        
        return None
    
    async def fetch_all_endpoints_for_case(self, session: aiohttp.ClientSession, cino: str) -> Dict:
        """Fetch data from all endpoints concurrently for a single case"""
        tasks = []
        
        # Create tasks for all endpoints
        if self.urls['interim_orders']:
            tasks.append(self._fetch_interim_orders(session, cino))
        
        if self.urls['acts_data']:
            tasks.append(self._fetch_acts_data(session, cino))
        
        if self.urls['cnr_data']:
            tasks.append(self._fetch_cnr_data(session, cino))
        
        if self.urls['case_history']:
            tasks.append(self._fetch_case_history(session, cino))
        
        if self.urls['judgments']:
            tasks.append(self._fetch_judgments(session, cino))
        
        if self.urls['documents']:
            tasks.append(self._fetch_documents(session, cino))
        
        if self.urls['connected_cases']:
            tasks.append(self._fetch_connected_cases(session, cino))
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results
        case_data = {"cino": cino}
        endpoint_names = ['interim_orders', 'acts_data', 'cnr_data', 'case_history', 
                         'judgments', 'documents', 'connected_cases']
        
        for i, result in enumerate(results):
            if i < len(endpoint_names) and not isinstance(result, Exception):
                case_data[endpoint_names[i]] = result
            else:
                case_data[endpoint_names[i]] = [] if isinstance(result, Exception) else result
        
        return case_data
    
    async def _fetch_interim_orders(self, session: aiohttp.ClientSession, cino: str) -> List[Dict]:
        """Fetch interim orders"""
        params = {"cino": cino}
        data = await self._make_request(session, 'GET', self.urls['interim_orders'], params=params)
        
        if not data:
            return []
        
        # Process order paths
        processed_orders = []
        for order in data:
            for path_key in ["order_path", "interim_order_path"]:
                if order.get(path_key):
                    absolute_url = self._decode_document_url(order[path_key])
                    if absolute_url:
                        new_key = f"absolute_{path_key.replace('_path', '')}_url"
                        order[new_key] = absolute_url
                    order.pop(path_key, None)
            processed_orders.append(order)
        
        return processed_orders
    
    async def _fetch_acts_data(self, session: aiohttp.ClientSession, cino: str) -> Dict:
        """Fetch acts data"""
        # Try PENDING first
        payload = {"cino": cino, "pending": "PENDING"}
        data = await self._make_request(session, 'POST', self.urls['acts_data'], json=payload)
        if data:
            return {"status": "PENDING", "acts": data}
        
        # Try DISPOSED
        payload = {"cino": cino, "pending": "DISPOSED"}
        data = await self._make_request(session, 'POST', self.urls['acts_data'], json=payload)
        if data:
            return {"status": "DISPOSED", "acts": data}
        
        return {"status": "NONE", "acts": []}
    
    async def _fetch_cnr_data(self, session: aiohttp.ClientSession, cino: str) -> Dict:
        """Fetch CNR data"""
        payload = {"cnr": cino}
        data = await self._make_request(session, 'POST', self.urls['cnr_data'], json=payload)
        return data or {}
    
    async def _fetch_case_history(self, session: aiohttp.ClientSession, cino: str) -> List[Dict]:
        """Fetch case history"""
        params = {"cino": cino}
        data = await self._make_request(session, 'GET', self.urls['case_history'], params=params)
        return data or []
    
    async def _fetch_judgments(self, session: aiohttp.ClientSession, cino: str) -> List[Dict]:
        """Fetch judgments"""
        params = {"cino": cino}
        data = await self._make_request(session, 'GET', self.urls['judgments'], params=params)
        return data or []
    
    async def _fetch_documents(self, session: aiohttp.ClientSession, cino: str) -> List[Dict]:
        """Fetch documents"""
        params = {"cino": cino}
        data = await self._make_request(session, 'GET', self.urls['documents'], params=params)
        return data or []
    
    async def _fetch_connected_cases(self, session: aiohttp.ClientSession, cino: str) -> List[Dict]:
        """Fetch connected cases"""
        params = {"cino": cino}
        data = await self._make_request(session, 'GET', self.urls['connected_cases'], params=params)
        return data or []
    
    def _decode_document_url(self, encoded_url: str) -> Optional[str]:
        """Decode document URL from base64 parameters"""
        try:
            parsed = urllib.parse.urlparse(encoded_url)
            params = urllib.parse.parse_qs(parsed.query)
            
            token = params.get('token', [None])[0]
            lookups = params.get('lookups', [None])[0]
            
            if not token or not lookups:
                return None
            
            decoded_token = base64.b64decode(token).decode('utf-8')
            decoded_lookups = base64.b64decode(lookups).decode('utf-8')
            
            relative_path = f"{decoded_lookups}/{decoded_token}"
            return urllib.parse.urljoin(self.base_url, relative_path)
            
        except Exception as e:
            logger.warning(f"Failed to decode document URL: {e}")
            return None
    
    async def process_cases_batch(self, session: aiohttp.ClientSession, cases_batch: List[Dict]) -> List[Dict]:
        """Process a batch of cases concurrently"""
        tasks = []
        
        for case in cases_batch:
            cino = case.get('cino', '').strip()
            if cino:
                task = self.fetch_all_endpoints_for_case(session, cino)
                tasks.append(task)
        
        if not tasks:
            return []
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and add metadata
        valid_results = []
        for i, result in enumerate(results):
            if not isinstance(result, Exception) and result:
                case = cases_batch[i]
                result.update({
                    'case_type': int(case.get('case_type', 0)),
                    'year': int(case.get('year', 0))
                })
                valid_results.append(result)
        
        return valid_results
    
    async def process_csv_cases_async(self, csv_filename: str, output_filename: str) -> List[Dict]:
        """Process all cases from CSV with resume capability and network recovery"""
        csv_path = OUTPUT_DIR / csv_filename
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return []
        
        # Load progress
        progress = self.load_progress(output_filename)
        processed_cinos_set = set(progress['processed_cinos'])
        
        # Read all cases
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            all_cases = [case for case in reader if case.get('cino', '').strip()]
        
        # Filter out already processed cases
        remaining_cases = [case for case in all_cases 
                          if case.get('cino', '').strip() not in processed_cinos_set]
        
        total_cases = len(all_cases)
        remaining_count = len(remaining_cases)
        
        logger.info(f"Total cases: {total_cases}")
        logger.info(f"Already processed: {total_cases - remaining_count}")
        logger.info(f"Remaining to process: {remaining_count}")
        
        if not remaining_cases:
            logger.info("All cases already processed!")
            return self.load_existing_results(output_filename)
        
        # Load existing results
        all_results = self.load_existing_results(output_filename)
        
        # Calculate batches
        total_batches = (remaining_count + self.max_concurrent - 1) // self.max_concurrent
        start_batch = progress['current_batch']
        
        logger.info(f"Processing with {self.max_concurrent} concurrent requests")
        logger.info(f"Starting from batch {start_batch + 1}/{total_batches}")
        
        # Configure session
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,
            limit_per_host=self.max_concurrent
        )
        
        try:
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Content-Type': 'application/json'
                }
            ) as session:
                
                # Process remaining cases in batches
                for i in range(start_batch * self.max_concurrent, remaining_count, self.max_concurrent):
                    batch_idx = i // self.max_concurrent
                    batch = remaining_cases[i:i + self.max_concurrent]
                    
                    logger.info(f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} cases)")
                    
                    # Check network before processing batch
                    if not await self.check_network_connectivity():
                        await self.wait_for_network_recovery()
                    
                    start_time = time.time()
                    
                    try:
                        batch_results = await self.process_cases_batch(session, batch)
                        batch_time = time.time() - start_time
                        
                        # Save batch results immediately
                        if batch_results:
                            all_results.extend(batch_results)
                            self.save_batch_results(output_filename, batch_results, append=True)
                        
                        # Update progress
                        processed_cinos_in_batch = [case['cino'] for case in batch if case.get('cino')]
                        progress['processed_cinos'].extend(processed_cinos_in_batch)
                        progress['current_batch'] = batch_idx + 1
                        progress['total_batches'] = total_batches
                        
                        self.save_progress(
                            output_filename, 
                            progress['processed_cinos'],
                            progress['current_batch'],
                            total_batches
                        )
                        
                        logger.info(f"Batch {batch_idx + 1} completed in {batch_time:.2f}s - {len(batch_results)} successful")
                        
                        # Rate limiting between batches
                        if i + self.max_concurrent < remaining_count:
                            await asyncio.sleep(self.delay_between_batches)
                    
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_idx + 1}: {e}")
                        # Don't update progress for failed batches
                        continue
        
        except KeyboardInterrupt:
            logger.info("Process interrupted by user. Progress has been saved.")
            return all_results
        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            return all_results
        
        # Mark as completed
        progress['completed'] = True
        self.save_progress(
            output_filename, 
            progress['processed_cinos'],
            total_batches,
            total_batches,
            completed=True
        )
        
        return all_results
    
    def finalize_results(self, output_filename: str):
        """Move temporary results to final output file and clean up"""
        temp_file = self.get_temp_results_file_path(output_filename)
        final_file = OUTPUT_DIR / output_filename
        progress_file = self.get_progress_file_path(output_filename)
        
        if temp_file.exists():
            # Move temp file to final location
            temp_file.replace(final_file)
            logger.info(f"Results finalized: {final_file}")
            
            # Clean up progress file
            if progress_file.exists():
                progress_file.unlink()
                logger.info("Progress tracking files cleaned up")
        else:
            logger.warning("No temporary results file found to finalize")
    
    def save_details_to_json(self, details: List[Dict], filename: str):
        """Save case details to JSON file"""
        if not details:
            logger.warning("No details to save")
            return
        
        filepath = OUTPUT_DIR / filename
        
        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(details, jsonfile, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(details)} case details to {filepath}")

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Async Kerala High Court Case Details Scraper with Resume')
    parser.add_argument('--input-csv', required=True, help='Input CSV filename')
    parser.add_argument('--output-json', default='case_details_async.json', help='Output JSON filename')
    parser.add_argument('--max-concurrent', type=int, default=10, help='Max concurrent requests')
    parser.add_argument('--batch-delay', type=float, default=1.0, help='Delay between batches (seconds)')
    parser.add_argument('--reset', action='store_true', help='Reset progress and start from beginning')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(OUTPUT_DIR / 'async_scraper.log'),
            logging.StreamHandler()
        ]
    )
    
    scraper = AsyncCaseDetailsScraper(
        max_concurrent=args.max_concurrent,
        delay_between_batches=args.batch_delay
    )
    
    # Reset progress if requested
    if args.reset:
        progress_file = scraper.get_progress_file_path(args.output_json)
        temp_file = scraper.get_temp_results_file_path(args.output_json)
        
        if progress_file.exists():
            progress_file.unlink()
            logger.info("Progress reset")
        if temp_file.exists():
            temp_file.unlink()
            logger.info("Temporary results cleared")
    
    try:
        start_time = time.time()
        details = await scraper.process_csv_cases_async(args.input_csv, args.output_json)
        total_time = time.time() - start_time
        
        # Finalize results
        scraper.finalize_results(args.output_json)
        
        logger.info(f"Processing complete in {total_time:.2f}s")
        logger.info(f"Total cases processed: {len(details)}")
        if details:
            logger.info(f"Average time per case: {total_time/len(details):.2f}s")
    
    except KeyboardInterrupt:
        logger.info("Process interrupted. You can resume later by running the same command.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.info("You can resume processing by running the same command.")

if __name__ == '__main__':
    asyncio.run(main())