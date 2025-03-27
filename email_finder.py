import re
import dns.resolver
import smtplib
import socket
import requests
import random
import logging
import json
import time
import pandas as pd
from typing import List, Dict, Optional
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import multiprocessing as mp
from functools import partial

class EmailFinder:
    def __init__(self):
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Email pattern templates
        self.email_patterns = [
            "{first}.{last}", "{first}{last}", "{f}{last}", 
            "{first}", "{first}{initial}", "{initial}{last}",
            "{first}_{last}", "{first}-{last}"
        ]
        
        # Configure DNS resolver
        self.resolver = dns.resolver.Resolver()
        self.resolver.nameservers = ['8.8.8.8', '8.8.4.4']  # Google DNS servers
        
        # Configure requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def generate_email_variations(self, first_name: str, last_name: str, domain: str) -> List[str]:
        """
        Generate potential email variations
        
        Args:
            first_name (str): First name
            last_name (str): Last name
            domain (str): Company domain
        
        Returns:
            List of potential email addresses
        """
        # Normalize inputs
        first = first_name.lower().replace(' ', '')
        last = last_name.lower().replace(' ', '')
        initial = first[0]
        
        # Clean domain to prevent duplicates
        clean_domain = domain.lower().replace('www.', '')
        if clean_domain.endswith('.com.com'):
            clean_domain = clean_domain[:-4]  # Remove the extra .com
        
        # Generate variations
        emails = []
        for pattern in self.email_patterns:
            email = pattern.format(
                first=first, 
                last=last, 
                f=first[0], 
                initial=initial
            )
            emails.append(f"{email}@{clean_domain}")
        
        return list(set(emails))  # Remove duplicates

    def verify_email_mx(self, email: str) -> bool:
        """
        Verify email by checking MX records
        
        Args:
            email (str): Email address to verify
        
        Returns:
            bool: Whether email domain has valid MX records
        """
        domain = email.split('@')[-1]
        try:
            dns.resolver.resolve(domain, 'MX')
            return True
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            return False

    def verify_email_smtp(self, email: str, timeout: float = 5.0) -> bool:
        """
        Verify email using strict SMTP verification
        
        Args:
            email (str): Email address to verify
            timeout (float): Connection timeout
        
        Returns:
            bool: Whether email appears to be valid
        """
        domain = email.split('@')[-1]
        
        # Skip common domains
        common_domains = {'google.com', 'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'medium.com'}
        if domain.lower() in common_domains:
            return False
            
        try:
            # Get MX record
            mx_records = dns.resolver.resolve(domain, 'MX')
            mx_record = str(mx_records[0].exchange)

            # Attempt SMTP connection
            with smtplib.SMTP(mx_record, 25, timeout=timeout) as smtp:
                smtp.ehlo()
                smtp.mail('')
                code, message = smtp.rcpt(str(email))
                
                # Log the response for debugging
                self.logger.debug(f"SMTP response for {email}: {code} - {message}")
                
                # Only accept code 250 (success)
                if code == 250:
                    self.logger.info(f"Found valid email: {email}")
                    return True
                # Reject all other codes
                return False
                    
        except Exception as e:
            self.logger.debug(f"SMTP verification failed for {email}: {e}")
            return False

    def find_company_domain(self, company_name: str) -> Optional[str]:
        """
        Find company domain through multiple methods
        
        Args:
            company_name (str): Company name
        
        Returns:
            Extracted domain or None
        """
        # Try direct domain lookup first
        domain = self._try_direct_domain_lookup(company_name)
        if domain and self._is_valid_company_domain(domain, company_name):
            return domain
            
        # Try Google search as fallback
        domain = self._try_google_search(company_name)
        if domain and self._is_valid_company_domain(domain, company_name):
            return domain
            
        return None
    
    def _try_direct_domain_lookup(self, company_name: str) -> Optional[str]:
        """
        Try to find domain by direct lookup
        
        Args:
            company_name (str): Company name
        
        Returns:
            Extracted domain or None
        """
        try:
            # Clean company name
            clean_name = company_name.lower().replace(' ', '')
            
            # Common domain extensions
            extensions = ['.com', '.org', '.net', '.io', '.co']
            
            for ext in extensions:
                domain = f"{clean_name}{ext}"
                try:
                    # Try to resolve the domain
                    self.resolver.resolve(domain, 'A')
                    return domain
                except dns.resolver.NXDOMAIN:
                    continue
                except Exception as e:
                    self.logger.debug(f"DNS lookup failed for {domain}: {e}")
                    continue
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Direct domain lookup failed: {e}")
            return None
    
    def _try_google_search(self, company_name: str) -> Optional[str]:
        """
        Try to find domain through Google search
        
        Args:
            company_name (str): Company name
        
        Returns:
            Extracted domain or None
        """
        try:
            # Prepare search query
            query = f"site:linkedin.com {company_name} official website"
            
            # Headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Perform search with configured session
            response = self.session.get(
                f"https://www.google.com/search?q={query}", 
                headers=headers,
                timeout=10
            )
            
            # Extract domain using regex
            domain_match = re.search(r'https?://(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})', response.text)
            
            return domain_match.group(1) if domain_match else None
        
        except Exception as e:
            self.logger.warning(f"Google search failed: {e}")
            return None

    def _is_valid_company_domain(self, domain: str, company_name: str) -> bool:
        """
        Check if domain is valid for the company
        
        Args:
            domain (str): Domain to check
            company_name (str): Company name
        
        Returns:
            bool: Whether domain is valid
        """
        # Skip common domains
        common_domains = {'google.com', 'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'medium.com'}
        if domain.lower() in common_domains:
            return False
            
        # Clean company name for comparison
        clean_company = company_name.lower().replace(' ', '')
        clean_domain = domain.lower().replace('www.', '')
        
        # Remove any duplicate domain extensions
        if clean_domain.endswith('.com.com'):
            clean_domain = clean_domain[:-4]  # Remove the extra .com
            
        # Check if company name is part of domain
        return clean_company in clean_domain

    def comprehensive_email_search(self, name: str, company: str) -> Dict:
        """
        Comprehensive email finding and verification process
        
        Args:
            name (str): Full name
            company (str): Company name
        
        Returns:
            Comprehensive email search results
        """
        # Split name
        try:
            first_name, last_name = name.split(' ', 1)
        except ValueError:
            return {'error': 'Invalid name format. Please provide first and last name.'}
        
        # Find company domain
        domain = self.find_company_domain(company)
        if not domain:
            return {'error': 'Could not determine company domain'}
        
        # Generate potential emails
        potential_emails = self.generate_email_variations(first_name, last_name, domain)
        
        # Log the potential emails being checked
        self.logger.info(f"Checking potential emails for {name} at {company}: {', '.join(potential_emails)}")
        
        # Try the most common format first (first.last@domain)
        preferred_email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
        if preferred_email in potential_emails:
            mx_verified = self.verify_email_mx(preferred_email)
            if mx_verified:
                time.sleep(1)  # Delay between MX and SMTP checks
                if self.verify_email_smtp(preferred_email):
                    self.logger.info(f"Found valid email: {preferred_email}")
                    return {
                        'domain': domain,
                        'valid_emails': [preferred_email]
                    }
        
        # If preferred format fails, try other formats
        verified_emails = {}
        valid_emails = []
        
        for email in potential_emails:
            if email == preferred_email:  # Skip the preferred email as we already checked it
                continue
                
            mx_verified = self.verify_email_mx(email)
            if mx_verified:
                time.sleep(1)  # Delay between MX and SMTP checks
                if self.verify_email_smtp(email):
                    valid_emails.append(email)
        
        if valid_emails:
            # Only return the first valid email found
            self.logger.info(f"Found valid email: {valid_emails[0]}")
            return {
                'domain': domain,
                'valid_emails': [valid_emails[0]]
            }
        else:
            self.logger.info(f"No valid emails found for {name} at {company}")
            return {
                'domain': domain,
                'valid_emails': []
            }

def process_chunk(chunk_data: pd.DataFrame, finder: EmailFinder, output_file: Path) -> None:
    """
    Process a chunk of data in parallel
    
    Args:
        chunk_data (pd.DataFrame): Chunk of data to process
        finder (EmailFinder): EmailFinder instance
        output_file (Path): Path to output CSV file
    """
    for _, row in chunk_data.iterrows():
        try:
            # Extract name and company from correct columns
            first_name = row['first_name']
            last_name = row['last_name']
            company = row['current_positions/0/companyName']
            
            # Skip if missing required data
            if pd.isna(first_name) or pd.isna(last_name) or pd.isna(company):
                continue
            
            # Find email
            result = finder.comprehensive_email_search(
                name=f"{first_name} {last_name}",
                company=company
            )
            
            # Get the most likely email (prefer first.last@domain format)
            email = None
            if result.get('valid_emails'):
                # Try to find first.last format first
                preferred_email = f"{first_name.lower()}.{last_name.lower()}@{result['domain']}"
                if preferred_email in result['valid_emails']:
                    email = preferred_email
                else:
                    # If preferred format not found, use the first valid email
                    email = result['valid_emails'][0]
            
            # Write to CSV with lock to prevent concurrent writes
            if email:
                with mp.Lock():
                    with open(output_file, 'a', newline='') as f:
                        f.write(f"{first_name},{last_name},{company},{email},{row.get('file', 'unknown')}\n")
            
            # Add small delay to avoid rate limits
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error processing row for {first_name} {last_name}: {str(e)}")
            continue

def main():
    # Initialize finder
    finder = EmailFinder()
    
    # Setup directories
    input_dir = Path('input')  # Fixed from 'continue' to 'input'
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    # Log directory paths
    logging.info(f"Looking for CSV files in: {input_dir.absolute()}")
    logging.info(f"Output directory: {output_dir.absolute()}")
    
    # Check if input directory exists and has CSV files
    if not input_dir.exists():
        logging.error(f"Input directory does not exist: {input_dir.absolute()}")
        return
        
    csv_files = list(input_dir.glob('*.csv'))
    if not csv_files:
        logging.error(f"No CSV files found in {input_dir.absolute()}")
        return
        
    logging.info(f"Found {len(csv_files)} CSV files to process")
    
    # Set to None to process all rows, or a number to limit rows (e.g., 25)
    ROW_LIMIT = None  # Change to 25 to test with fewer rows
    
    # Create output CSV with headers
    output_file = output_dir / "email_results.csv"
    with open(output_file, 'w', newline='') as f:
        f.write("firstname,lastname,company,email,file\n")
    
    # Set up multiprocessing pool with 4 CPUs
    num_processes = 4
    logging.info(f"Starting multiprocessing pool with {num_processes} processes")
    pool = mp.Pool(processes=num_processes)
    
    # Process all CSV files in input directory
    for csv_file in csv_files:
        try:
            logging.info(f"Processing file: {csv_file.name}")
            # Read CSV file
            df = pd.read_csv(csv_file)
            logging.info(f"Read {len(df)} rows from {csv_file.name}")
            
            # Limit rows if specified
            if ROW_LIMIT is not None:
                df = df.head(ROW_LIMIT)
                logging.info(f"Limited to first {ROW_LIMIT} rows")
            
            # Add file name to each row
            df['file'] = csv_file.name
            
            # Split DataFrame into chunks for parallel processing
            chunk_size = len(df) // num_processes
            if chunk_size == 0:
                chunk_size = 1
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            logging.info(f"Split into {len(chunks)} chunks of size {chunk_size}")
            
            # Process chunks in parallel
            process_chunk_partial = partial(process_chunk, finder=finder, output_file=output_file)
            pool.map(process_chunk_partial, chunks)
            
            logging.info(f"Completed processing {csv_file.name}")
            
        except Exception as e:
            logging.error(f"Error processing file {csv_file.name}: {str(e)}")
            continue
    
    # Close the pool
    logging.info("Closing multiprocessing pool")
    pool.close()
    pool.join()
    logging.info("Processing complete")

if __name__ == "__main__":
    main()