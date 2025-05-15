"""
Fast LoopNet Scraper using Playwright with concurrent scraping
Includes phone number extraction
"""

import asyncio
import json
import csv
import time
import random
from datetime import datetime
from pathlib import Path
import re
import logging
from urllib.parse import urljoin
from typing import Dict, List, Any, Optional
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PlaywrightLoopNetScraper:
    def __init__(self):
        # Basic configuration
        self.search_url = "https://www.loopnet.com/search/commercial-real-estate/new-york-ny/for-sale/"
        self.page_limit = 20  # Number of search results pages to scrape
        self.max_concurrent = 10  # Max number of concurrent property scrapes
        self.wait_time = 1  # Minimum wait time between actions
        
        # Data directories
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
        # Data storage
        self.listing_urls = []
        self.results = []
        
        # Initialize browser/context/page as None
        self.browser = None
        self.context = None
        self.search_page = None
    
    async def run(self):
        """Main method to run the scraper"""
        try:
            async with async_playwright() as p:
                # Launch browser
                logger.info("Launching browser...")
                self.browser = await p.chromium.launch(
                    headless=False,
                    channel="chrome",
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--start-maximized'
                    ]
                )
                
                # Create browser context
                self.context = await self.browser.new_context(
                    viewport=None,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                
                # Create page and start with Google (helps avoid detection)
                self.search_page = await self.context.new_page()
                await self.search_page.goto("https://www.google.com", wait_until="domcontentloaded")
                await asyncio.sleep(0.5)
                
                # Gather listing URLs from search pages
                await self.gather_listing_urls()
                
                # Concurrently scrape individual property listings
                if self.listing_urls:
                    logger.info(f"Found {len(self.listing_urls)} property listings to scrape")
                    await self.scrape_property_listings()
                else:
                    logger.warning("No property listings found to scrape")
                
                # Save results
                self.save_results()
                
                # Close browser
                await self.search_page.close()
                await self.context.close()
                await self.browser.close()
        
        except Exception as e:
            logger.error(f"An error occurred during scraping: {str(e)}")
            # Attempt to close browser in case of error
            try:
                if self.search_page:
                    await self.search_page.close()
                if self.context:
                    await self.context.close()
                if self.browser:
                    await self.browser.close()
            except:
                pass
            raise
    
    async def gather_listing_urls(self):
        """Collect all property listing URLs from search results pages"""
        for page_num in range(1, self.page_limit + 1):
            try:
                # Construct URL for current page
                if page_num == 1:
                    url = self.search_url
                else:
                    url = f"{self.search_url}?page={page_num}"
                
                logger.info(f"Navigating to search page {page_num}: {url}")
                
                # Navigate to search page
                await self.search_page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(1)  # Short wait for content to load
                
                # Wait for listing results container
                await self.search_page.wait_for_selector('#placardSec > div.placards', timeout=20000)
                
                # Extract listing URLs
                links = await self._extract_listing_urls(self.search_page, page_num)
                
                if links:
                    logger.info(f"Found {len(links)} links on search page {page_num}")
                    self.listing_urls.extend(links)
                else:
                    logger.warning(f"No links found on search page {page_num}")
                
                # Brief pause between pages
                if page_num < self.page_limit:
                    await asyncio.sleep(self.wait_time)
                
            except Exception as e:
                logger.error(f"Error on search page {page_num}: {str(e)}")
    
    async def _extract_listing_urls(self, page: Page, page_num: int) -> List[Dict[str, str]]:
        """Extract property listing URLs from a search results page"""
        try:
            # Use JavaScript to extract URLs directly from the page
            links = await page.evaluate("""
                () => {
                    // Try different selectors to find property listings
                    const selectors = [
                        'ul li article div a[href*="/Listing/"]',
                        'a[title^="More details for"]',
                        'a[href*="/Listing/"]',
                        '.placard-link',
                        '.title-link'
                    ];
                    
                    let result = [];
                    
                    // Try each selector
                    for (const selector of selectors) {
                        const elements = document.querySelectorAll(selector);
                        if (elements.length > 0) {
                            result = Array.from(elements).map(a => {
                                return {
                                    url: a.href,
                                    title: a.textContent?.trim() || a.title || ''
                                };
                            }).filter(item => item.url && item.url.includes('loopnet.com'));
                            
                            if (result.length > 0) break;
                        }
                    }
                    
                    return result;
                }
            """)
            
            # Remove duplicates
            seen_urls = set()
            unique_links = []
            
            for link in links:
                if link["url"] not in seen_urls:
                    seen_urls.add(link["url"])
                    unique_links.append(link)
            
            return unique_links
            
        except Exception as e:
            logger.error(f"Error extracting listing URLs from page {page_num}: {str(e)}")
            return []
    
    async def scrape_property_listings(self):
        """Scrape individual property listings concurrently"""
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Create tasks for each property listing
        tasks = []
        for i, property_link in enumerate(self.listing_urls):
            task = self.scrape_property_listing(
                property_link=property_link,
                property_num=i+1,
                semaphore=semaphore
            )
            tasks.append(task)
        
        # Run tasks concurrently and gather results
        logger.info(f"Starting concurrent scraping of {len(tasks)} properties...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        success_count = sum(1 for r in results if r is not None and not isinstance(r, Exception))
        logger.info(f"Successfully scraped {success_count} properties out of {len(tasks)}")
    
    async def scrape_property_listing(self, property_link: Dict[str, str], property_num: int, semaphore: asyncio.Semaphore):
        """Scrape a single property listing with concurrency control"""
        async with semaphore:
            property_url = property_link.get("url")
            property_title = property_link.get("title", "Unknown Property")
            
            if not property_url:
                return None
            
            logger.info(f"[{property_num}] Scraping property: {property_title}")
            logger.info(f"URL: {property_url}")
            
            try:
                # Create a new page for this property
                property_page = await self.context.new_page()
                
                try:
                    # Navigate to property page
                    await property_page.goto(property_url, wait_until="domcontentloaded", timeout=30000)
                    
                    # Wait for important content to load
                    try:
                        await property_page.wait_for_selector('section[class*="listing-features"]', timeout=5000)
                    except:
                        # If specific selector fails, wait for any content
                        await asyncio.sleep(2)
                    
                    # Extract and process property data
                    property_data = await self._extract_property_data(property_page, property_url)
                    
                    # Extract phone number
                    phone_number = await self._extract_phone_number(property_page)
                    if phone_number:
                        property_data['broker_phone'] = phone_number
                    
                    if property_data:
                        # Add to results list
                        self.results.append(property_data)
                        logger.info(f"Successfully extracted data for property {property_num}")
                        return property_data
                    else:
                        logger.warning(f"No data extracted for property {property_num}")
                        return None
                    
                finally:
                    # Always close the page when done
                    await property_page.close()
            
            except Exception as e:
                logger.error(f"Error scraping property {property_num} ({property_url}): {str(e)}")
                return None
    
    async def _extract_phone_number(self, page: Page) -> Optional[str]:
        """Extract broker phone number from property page"""
        try:
            # Method 1: Try to find the phone number button and click it if needed
            phone_button_selector = '#dataSection div.container-contact-form.has-valid-contacts div span button'
            
            # Check if the button exists
            button_exists = await page.evaluate(f"""
                () => {{
                    const button = document.querySelector('{phone_button_selector}');
                    return button !== null;
                }}
            """)
            
            if button_exists:
                # First try to see if phone number is already visible
                phone_number = await page.evaluate("""
                    () => {
                        // Try various selectors for phone numbers
                        const selectors = [
                            '.contact-phone',
                            '.broker-phone',
                            '.contact-info-phone',
                            'div[class*="phone"]',
                            'a[href^="tel:"]'
                        ];
                        
                        for (const selector of selectors) {
                            const elements = document.querySelectorAll(selector);
                            for (const el of elements) {
                                const text = el.textContent.trim();
                                // Check if it matches a phone number pattern
                                if (/^[\d\s\(\)\.\-\+]+$/.test(text) && text.length >= 7) {
                                    return text;
                                }
                                
                                // Check for href attribute
                                if (el.href && el.href.startsWith('tel:')) {
                                    return el.href.replace('tel:', '');
                                }
                            }
                        }
                        
                        return null;
                    }
                """)
                
                if not phone_number:
                    # Click the button to reveal the phone number
                    try:
                        await page.click(phone_button_selector)
                        # Wait a moment for the number to appear
                        await asyncio.sleep(0.5)
                        
                        # Now try to extract the revealed phone number
                        phone_number = await page.evaluate("""
                            () => {
                                // Check for newly revealed phone elements
                                const selectors = [
                                    '.contact-phone',
                                    '.broker-phone',
                                    '.contact-info-phone',
                                    'div[class*="phone"]',
                                    'a[href^="tel:"]',
                                    'span[class*="phone"]'
                                ];
                                
                                for (const selector of selectors) {
                                    const elements = document.querySelectorAll(selector);
                                    for (const el of elements) {
                                        const text = el.textContent.trim();
                                        // Check if it matches a phone number pattern
                                        if (/^[\d\s\(\)\.\-\+]+$/.test(text) && text.length >= 7) {
                                            return text;
                                        }
                                        
                                        // Check for href attribute
                                        if (el.href && el.href.startsWith('tel:')) {
                                            return el.href.replace('tel:', '');
                                        }
                                    }
                                }
                                
                                return null;
                            }
                        """)
                    except Exception as e:
                        logger.warning(f"Error clicking phone button: {str(e)}")
                
                return phone_number
            
            # Method 2: Alternative approach - look for phone patterns in the page
            phone_number = await page.evaluate("""
                () => {
                    // Look for elements that might contain phone numbers
                    const textNodes = [];
                    const walker = document.createTreeWalker(
                        document.body, 
                        NodeFilter.SHOW_TEXT,
                        null,
                        false
                    );
                    
                    let node;
                    while (node = walker.nextNode()) {
                        const text = node.nodeValue.trim();
                        // Simple phone number pattern check
                        if (/^[\d\s\(\)\.\-\+]{7,20}$/.test(text)) {
                            textNodes.push(text);
                        }
                    }
                    
                    // Also check for tel: links
                    const telLinks = document.querySelectorAll('a[href^="tel:"]');
                    for (const link of telLinks) {
                        textNodes.push(link.href.replace('tel:', ''));
                    }
                    
                    return textNodes.length > 0 ? textNodes[0] : null;
                }
            """)
            
            return phone_number
            
        except Exception as e:
            logger.error(f"Error extracting phone number: {str(e)}")
            return None
    
    async def _extract_property_data(self, page: Page, url: str) -> Optional[Dict[str, Any]]:
        """Extract detailed data from a property listing page"""
        try:
            # Initialize item with basic data
            item = {
                "listing_url": url,
                "listing_id": self._extract_listing_id(url),
                "scraped_at": datetime.now().isoformat()
            }
            
            # Extract JSON-LD data from script tag
            json_ld_script = await page.evaluate("""
                () => {
                    // First try specific XPath
                    const scriptXPath = document.evaluate(
                        '/html/body/section[1]/main/section/script[2]',
                        document,
                        null,
                        XPathResult.FIRST_ORDERED_NODE_TYPE,
                        null
                    ).singleNodeValue;
                    
                    if (scriptXPath) {
                        return scriptXPath.textContent;
                    }
                    
                    // Alternative: Try to find JSON-LD script
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const script of scripts) {
                        const content = script.textContent;
                        if (content && content.includes('"@type":"Apartment"') || 
                            content.includes('"@type":"RealEstateListing"') ||
                            content.includes('"@type":"Place"')) {
                            return content;
                        }
                    }
                    
                    // Backup: Try to find any script in the main section
                    const mainScripts = document.querySelectorAll('main section script');
                    for (const script of mainScripts) {
                        const content = script.textContent;
                        if (content && (content.startsWith('{') || content.startsWith('['))) {
                            return content;
                        }
                    }
                    
                    return null;
                }
            """)
            
            if json_ld_script:
                try:
                    # Parse JSON data
                    json_data = json.loads(json_ld_script)
                    item["json_ld_data"] = json_ld_script
                    
                    # Extract specific fields from JSON-LD
                    if 'offers' in json_data and len(json_data['offers']) > 0:
                        offer = json_data['offers'][0]
                        item['price'] = offer.get('price')
                        item['price_currency'] = offer.get('priceCurrency')
                    
                    # Extract property details from additionalProperty
                    if 'additionalProperty' in json_data:
                        for prop in json_data['additionalProperty']:
                            name = prop.get('name', '').lower()
                            value = prop.get('value', [])
                            
                            if not value:
                                continue
                                
                            if 'property type' in name:
                                item['property_type'] = value[0] if isinstance(value, list) else value
                            elif 'property subtype' in name:
                                item['property_subtype'] = value[0] if isinstance(value, list) else value
                            elif 'price per unit' in name:
                                item['price_per_unit'] = value[0] if isinstance(value, list) else value
                            elif 'sale type' in name:
                                item['sale_type'] = value[0] if isinstance(value, list) else value
                            elif 'sale conditions' in name:
                                item['sale_conditions'] = ', '.join(value) if isinstance(value, list) else value
                            elif 'no. units' in name or 'num units' in name:
                                item['num_units'] = value[0] if isinstance(value, list) else value
                            elif 'building class' in name:
                                item['building_class'] = value[0] if isinstance(value, list) else value
                            elif 'lot size' in name:
                                item['lot_size'] = value[0] if isinstance(value, list) else value
                            elif 'building size' in name:
                                item['building_size'] = value[0] if isinstance(value, list) else value
                            elif 'occupancy' in name:
                                item['occupancy'] = value[0] if isinstance(value, list) else value
                            elif 'no. stories' in name or 'num stories' in name:
                                item['num_stories'] = value[0] if isinstance(value, list) else value
                            elif 'year built' in name:
                                item['year_built'] = value[0] if isinstance(value, list) else value
                            elif 'zoning' in name:
                                item['zoning'] = value[0] if isinstance(value, list) else value
                            elif 'amenities' in name:
                                item['amenities'] = ', '.join(value) if isinstance(value, list) else value
                            elif 'walk score' in name:
                                item['walk_score'] = value if isinstance(value, (int, float)) else value[0] if isinstance(value, list) else value
                    
                    # Extract address information
                    if 'contentLocation' in json_data and 'address' in json_data['contentLocation']:
                        address = json_data['contentLocation']['address']
                        item['street_address'] = address.get('streetAddress')
                        item['address_locality'] = address.get('addressLocality')
                        item['address_region'] = address.get('addressRegion')
                        item['postal_code'] = address.get('postalCode')
                        item['address_country'] = address.get('addressCountry')
                    
                    # Extract broker information
                    if 'provider' in json_data and len(json_data['provider']) > 0:
                        provider = json_data['provider'][0]
                        item['broker_name'] = provider.get('name')
                        if 'memberOf' in provider:
                            item['broker_company'] = provider['memberOf'].get('name')
                        if 'image' in provider and 'url' in provider['image']:
                            item['broker_image'] = provider['image'].get('url')
                        item['broker_profile_url'] = provider.get('@id')
                    
                    # Extract description and images
                    item['description'] = json_data.get('description')
                    item['images'] = json_data.get('image')
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON-LD data for {url}: {str(e)}")
            
            return item
            
        except Exception as e:
            logger.error(f"Error extracting property data: {str(e)}")
            return None
    
    def _extract_listing_id(self, url: str) -> Optional[str]:
        """Extract listing ID from URL"""
        parts = url.split('/')
        for i, part in enumerate(parts):
            if part == 'Listing' and i + 1 < len(parts):
                return parts[i + 1]
        return None
    
    def save_results(self):
        """Save scraped results to JSON and CSV files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if not self.results:
            logger.warning("No results to save")
            return
        
        # Save as JSON
        json_file = self.output_dir / f'loopnet_listings_{timestamp}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        # Save as CSV
        csv_file = self.output_dir / f'loopnet_listings_{timestamp}.csv'
        fieldnames = set()
        for item in self.results:
            fieldnames.update(item.keys())
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(self.results)
        
        logger.info(f'Saved {len(self.results)} items to {self.output_dir}')
        logger.info(f'JSON file: {json_file}')
        logger.info(f'CSV file: {csv_file}')
        
        # Print summary of phone numbers found
        phone_count = sum(1 for item in self.results if 'broker_phone' in item and item['broker_phone'])
        logger.info(f'Found phone numbers for {phone_count} out of {len(self.results)} listings')


async def main():
    """Main function to run the scraper"""
    scraper = PlaywrightLoopNetScraper()
    await scraper.run()


if __name__ == "__main__":
    import sys
    
    print("Starting Fast LoopNet Scraper with Playwright...")
    print("This will open Chrome and scrape property listings with phone numbers concurrently")
    
    # Set custom search URL and page limit from command line args
    if len(sys.argv) > 1:
        scraper = PlaywrightLoopNetScraper()
        scraper.search_url = sys.argv[1]
        if len(sys.argv) > 2:
            scraper.page_limit = int(sys.argv[2])
        
        print(f"Search URL: {scraper.search_url}")
        print(f"Page limit: {scraper.page_limit}")
        
        asyncio.run(scraper.run())
    else:
        # Run with default settings
        asyncio.run(main())