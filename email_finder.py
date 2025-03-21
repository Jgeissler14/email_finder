#!/usr/bin/env python3

# Email finder using Python for LinkedIn data CSV format
# Modified to only output verified emails with minimal data

import re
import sys
import socket
import smtplib
import pandas as pd
import dns.resolver
import logging
import time
import random
from email.utils import parseaddr
from concurrent.futures import ThreadPoolExecutor

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("email_finder.log"),
        logging.StreamHandler()
    ]
)

# Cache for domain verification results
domain_cache = {}
# Cache for known good/bad email patterns by domain
pattern_cache = {}
# Rate limiting parameters
last_request_time = {}
min_request_interval = 2  # seconds between requests to same domain

def formats(first, last, domain):
    """Create a list of possible email formats."""
    email_list = []

    # Convert names to lowercase for email generation
    first = first.lower().strip()
    last = last.lower().strip()
    
    # Remove accents and special characters
    first = re.sub(r'[^a-z0-9]', '', first)
    last = re.sub(r'[^a-z0-9]', '', last)
    
    # Skip if names are empty after cleaning
    if not first or not last:
        return email_list
    
    # Check if we have a pattern cache for this domain
    if domain in pattern_cache and pattern_cache[domain]:
        pattern = pattern_cache[domain]
        logging.debug(f"Using cached pattern {pattern} for {domain}")
        
        # Apply the known pattern
        if pattern == "first.last":
            email_list.append(f"{first}.{last}@{domain}")
        elif pattern == "flast":
            email_list.append(f"{first[0]}{last}@{domain}")
        elif pattern == "firstlast":
            email_list.append(f"{first}{last}@{domain}")
        elif pattern == "f.last":
            email_list.append(f"{first[0]}.{last}@{domain}")
        elif pattern == "first":
            email_list.append(f"{first}@{domain}")
        return email_list
    
    # Ordered by likelihood based on common business email patterns
    email_list = [
        f"{first}.{last}@{domain}",       # first.last@example.com
        f"{first[0]}{last}@{domain}",      # flast@example.com
        f"{first}{last}@{domain}",         # firstlast@example.com
        f"{first[0]}.{last}@{domain}",     # f.last@example.com
        f"{first}@{domain}",               # first@example.com
        f"{last}.{first}@{domain}",        # last.first@example.com
        f"{last}{first[0]}@{domain}",      # lastf@example.com
        f"{first}-{last}@{domain}",        # first-last@example.com
        f"{first}_{last}@{domain}"         # first_last@example.com
    ]
    
    return email_list

def verify_domain(domain):
    """Check if a domain has valid MX records and can receive email."""
    if domain in domain_cache:
        return domain_cache[domain]
    
    try:
        # Check MX records
        records = dns.resolver.resolve(domain, 'MX')
        if not records:
            logging.warning(f"No MX records found for {domain}")
            domain_cache[domain] = False
            return False
            
        # Try connecting to the mail server
        mx_record = str(records[0].exchange)
        smtp_server = mx_record.rstrip('.')
        
        # Rate limiting
        if smtp_server in last_request_time:
            elapsed = time.time() - last_request_time[smtp_server]
            if elapsed < min_request_interval:
                time.sleep(min_request_interval - elapsed + random.uniform(0.1, 1.0))
        
        server = smtplib.SMTP(timeout=10)
        server.set_debuglevel(0)
        server.connect(smtp_server)
        server.helo('example.com')
        server.quit()
        
        # Update last request time
        last_request_time[smtp_server] = time.time()
        
        logging.info(f"Domain {domain} has valid mail server: {smtp_server}")
        domain_cache[domain] = True
        return True
    
    except Exception as e:
        logging.warning(f"Domain verification failed for {domain}: {str(e)}")
        domain_cache[domain] = False
        return False

def verify_email_smtp(email, domain):
    """Check if an email address exists via SMTP."""
    try:
        # Parse email to ensure proper format
        parsed = parseaddr(email)[1]
        if not parsed:
            return False
            
        # Get MX records
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(records[0].exchange)
        smtp_server = mx_record.rstrip('.')
        
        # Rate limiting
        if smtp_server in last_request_time:
            elapsed = time.time() - last_request_time[smtp_server]
            if elapsed < min_request_interval:
                time.sleep(min_request_interval - elapsed + random.uniform(0.1, 1.0))
        
        # Connect to the SMTP server
        server = smtplib.SMTP(timeout=10)
        server.set_debuglevel(0)
        server.connect(smtp_server)
        server.helo('example.com')
        
        # Try RCPT TO command with a fake sender
        server.mail('verification@example.com')
        code, message = server.rcpt(email)
        server.quit()
        
        # Update last request time
        last_request_time[smtp_server] = time.time()
        
        # Check if RCPT TO was accepted
        if code == 250:
            logging.info(f"Email {email} verified via RCPT TO.")
            
            # Record the pattern for this domain
            name_parts = email.split('@')[0]
            if '.' in name_parts:
                pattern_cache[domain] = "first.last"
            elif '_' in name_parts:
                pattern_cache[domain] = "first_last"
            elif '-' in name_parts:
                pattern_cache[domain] = "first-last"
            elif len(name_parts) <= 6:  # Rough heuristic for first initial + last name
                pattern_cache[domain] = "flast"
            else:
                pattern_cache[domain] = "firstlast"
                
            return True
        else:
            logging.debug(f"Email {email} rejected: {message}")
            return False
            
    except Exception as e:
        logging.debug(f"SMTP verification failed for {email}: {str(e)}")
        return False

def extract_domain_from_company(company_name):
    """Extract a potential domain from company name."""
    if not company_name or pd.isna(company_name) or company_name.lower() == 'nan':
        return None
        
    # Remove non-alphanumeric chars except spaces
    company = re.sub(r'[^\w\s]', '', company_name.lower())
    # Replace spaces with empty string
    company = re.sub(r'\s+', '', company)
    
    # Try common TLDs
    domains = [
        f"{company}.com",
        f"{company}.io",
        f"{company}.co",
        f"{company}.net",
        f"{company}.org"
    ]
    
    # Try each domain
    for domain in domains:
        if verify_domain(domain):
            logging.info(f"Found working domain {domain} for {company_name}")
            return domain
    
    # No working domain found
    logging.warning(f"No working domain found for {company_name}")
    return f"{company}.com"  # Default to .com if no working domain found

def extract_domain_from_linkedin_url(url):
    """Extract domain from LinkedIn profile or company URL."""
    if not url or pd.isna(url) or url.lower() == 'nan':
        return None
        
    try:
        # Try to extract company domain from LinkedIn company URL
        company_url_match = re.search(r'linkedin\.com/company/([^/]+)', url)
        if company_url_match:
            company_slug = company_url_match.group(1)
            # Convert slug to potential domain
            domain = re.sub(r'[^\w]', '', company_slug.lower()) + ".com"
            if verify_domain(domain):
                return domain
            
        # Try to extract email domain from profile URL if present
        email_match = re.search(r'[\w\.-]+@([\w\.-]+)', url)
        if email_match:
            domain = email_match.group(1)
            if verify_domain(domain):
                return domain
                
        return None
    except Exception as e:
        logging.error(f"Error extracting domain from LinkedIn URL: {str(e)}")
        return None

def get_company_info_from_linkedin_data(row):
    """Extract company information from LinkedIn data fields."""
    company_info = {
        'name': None,
        'domain': None,
        'industry': None
    }
    
    # Try to get company name from different possible fields
    if 'company_name' in row and row['company_name'] and not pd.isna(row['company_name']):
        company_info['name'] = row['company_name']
    elif 'linkedin_company' in row and row['linkedin_company'] and not pd.isna(row['linkedin_company']):
        company_info['name'] = row['linkedin_company']
    elif 'current_positions/0/companyName' in row and row['current_positions/0/companyName'] and not pd.isna(row['current_positions/0/companyName']):
        company_info['name'] = row['current_positions/0/companyName']
    
    # Try to extract industry if available
    if 'current_positions/0/companyUrnResolutionResult/industry' in row and row['current_positions/0/companyUrnResolutionResult/industry'] and not pd.isna(row['current_positions/0/companyUrnResolutionResult/industry']):
        company_info['industry'] = row['current_positions/0/companyUrnResolutionResult/industry']
    
    # Try to get company domain from LinkedIn URL if available
    if 'linkedin' in row and row['linkedin'] and not pd.isna(row['linkedin']):
        domain = extract_domain_from_linkedin_url(row['linkedin'])
        if domain:
            company_info['domain'] = domain
    
    return company_info

def verify_emails(email_list, domain):
    """Verify a list of email addresses and return valid ones."""
    valid_emails = []
    
    # First check if domain is valid
    if not verify_domain(domain):
        logging.warning(f"Domain {domain} is not valid for email")
        return valid_emails
    
    # Try each email
    for email in email_list:
        if verify_email_smtp(email, domain):
            valid_emails.append(email)
            # We found one valid email, can stop checking more formats
            break
    
    return valid_emails

def process_row(row_data):
    """Process a single row from the dataframe."""
    i, row = row_data
    result = {
        'first_name': '',
        'last_name': '',
        'company_name': '',
        'email': '',
        'is_verified': False
    }
    
    try:
        # Extract person data
        first_name = str(row.get('first_name', ''))
        if pd.isna(first_name) or first_name.lower() == 'nan':
            first_name = ''
            
        last_name = str(row.get('last_name', ''))
        if pd.isna(last_name) or last_name.lower() == 'nan':
            last_name = ''
            
        # Get company information from LinkedIn data
        company_info = get_company_info_from_linkedin_data(row)
        company_name = company_info['name']
        
        # Store basic info
        result['first_name'] = first_name
        result['last_name'] = last_name
        result['company_name'] = company_name if company_name else ''
        
        logging.info(f"Processing row {i+1}: {first_name} {last_name} at {company_name}")
        
        # Skip if missing required data
        if not first_name or not last_name or not company_name:
            logging.warning(f"Row {i+1}: Missing required data, skipping")
            return i, result
        
        # Try to get domain
        domain = None
        
        # First try domain from LinkedIn URL extraction
        if company_info['domain']:
            domain = company_info['domain']
            logging.info(f"Using domain from LinkedIn URL: {domain}")
        
        # If no domain yet, try LinkedIn company URL if available
        if not domain and 'linkedin' in row and row['linkedin'] and not pd.isna(row['linkedin']):
            domain = extract_domain_from_linkedin_url(row['linkedin'])
            if domain:
                logging.info(f"Extracted domain from LinkedIn URL: {domain}")
        
        # If still no domain, extract from company name
        if not domain and company_name:
            domain = extract_domain_from_company(company_name)
            logging.info(f"Extracted domain from company name: {domain}")
        
        if not domain:
            return i, result
            
        # Generate email formats
        email_list = formats(first_name, last_name, domain)
        
        # Verify emails
        valid_emails = verify_emails(email_list, domain)
        
        # Update results
        if valid_emails:
            chosen_email = valid_emails[0]
            logging.info(f"Found valid email: {chosen_email}")
            result['email'] = chosen_email
            result['is_verified'] = True
            
    except Exception as e:
        logging.error(f"Error processing row {i+1}: {str(e)}", exc_info=True)
    
    return i, result

def process_csv(input_file, threads=4):
    """Process CSV file and find emails for each contact."""
    try:
        # Read the CSV
        logging.info(f"Reading CSV file: {input_file}")
        df = pd.read_csv(input_file)
        logging.info(f"Loaded CSV with {len(df)} rows and columns: {len(df.columns)} columns")
        
        # Process rows in parallel
        all_results = []
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for i, result in executor.map(process_row, [(i, row) for i, row in df.iterrows()]):
                all_results.append(result)
        
        # Create dataframe with verified emails only
        verified_results = [r for r in all_results if r['is_verified']]
        
        if not verified_results:
            logging.info("No verified emails found")
            return
            
        verified_df = pd.DataFrame(verified_results)
        verified_df = verified_df[['first_name', 'last_name', 'company_name', 'email']]
        
        # Save results to CSV
        output_file = input_file.replace('.csv', '_verified_emails.csv')
        if output_file == input_file:
            output_file = "verified_emails_" + input_file
        
        logging.info(f"Saving {len(verified_df)} verified emails to {output_file}")
        verified_df.to_csv(output_file, index=False)
        
        # Also output to console
        print("\nVerified Emails:\n")
        print(verified_df.to_string(index=False))
        print(f"\nTotal verified emails found: {len(verified_df)}")
        
    except Exception as e:
        logging.error(f"Error processing CSV: {e}", exc_info=True)

if __name__ == "__main__":
    logging.info("===== Verified Email Finder Script Started =====")
    
    # Get input file from command line or use default
    input_file = "software-11-50-page1.csv"
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Get number of threads from command line or use default
    threads = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    
    process_csv(input_file, threads)
    logging.info("===== Verified Email Finder Script Completed =====")