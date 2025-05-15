import asyncio
import json
import csv
import random
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List, Any, Optional
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import time
import string
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedYellowPagesScraper:
    def __init__(self, search_term="restaurants", location="los-angeles-ca", page_limit=1):
        # Core configuration
        self.search_term = search_term
        self.location = location
        self.search_url = f"https://www.yellowpages.com/{location}/{search_term}"
        self.page_limit = page_limit
        self.links_per_agent = 1
        self.search_page_delay = random.randint(5, 10)  # Randomized delay
        self.batch_size = 8
        self.timeout = 90000
        
        # Path setup
        self.output_dir = Path("yellowpages_data")
        self.output_dir.mkdir(exist_ok=True)
        
        # Data stores
        self.listing_urls = []
        self.results = []
        
        # Enhanced user agents with more variety
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        ]
        
        # Add proxy configuration (you need to provide your own proxies)
        self.proxies = [
            # Add your proxy list here in format: "http://username:password@host:port"
            # "http://user1:pass1@proxy1.example.com:8080",
            # "http://user2:pass2@proxy2.example.com:8080",
        ]
        
        self.browser = None
        
        # Request throttling
        self.min_request_interval = 3  # Minimum seconds between requests
        self.max_request_interval = 8  # Maximum seconds between requests
        self.last_request_time = 0

    async def scrape_more_info_section(self, page: Page) -> Dict:
        """Scrape the detailed 'More Info' section with enhanced deduplication"""
        result = {}
        
        try:
            # Find and extract the business info section
            more_info = await page.evaluate("""() => {
                // Possible container selectors for the business info section
                const containers = [
                    document.getElementById('business-info'),
                    document.querySelector('.business-details'),
                    document.querySelector('.business-info'),
                    document.querySelector('.more-info'),
                    document.querySelector('.information-section'),
                    document.querySelector('section#business-info'),
                    document.querySelector('dl')  // Sometimes the data is in a definition list
                ].filter(Boolean);
                
                if (!containers.length) return null;
                
                const result = {};
                
                // Process each container looking for key-value pairs
                for (const container of containers) {
                    // Look for dt/dd pairs (definition lists)
                    const terms = container.querySelectorAll('dt');
                    for (const term of terms) {
                        const definition = term.nextElementSibling;
                        if (definition && definition.tagName === 'DD') {
                            const key = term.textContent.trim().toLowerCase().replace(/\\s+/g, '_').replace(/[^a-z0-9_]/g, '');
                            const value = definition.textContent.trim();
                            if (key && value) {
                                result[key] = value;
                            }
                            
                            // Check specifically for email
                            if (definition.querySelector('a[href^="mailto:"]')) {
                                result['email'] = definition.querySelector('a[href^="mailto:"]').href.replace('mailto:', '');
                            }
                            
                            // Look for website links
                            const websiteLinks = Array.from(definition.querySelectorAll('a:not([href^="mailto:"])'))
                                .filter(a => 
                                    a.href && 
                                    !a.href.includes('yellowpages.com') && 
                                    !a.href.startsWith('tel:') &&
                                    !a.href.startsWith('#')
                                );
                                
                            if (websiteLinks.length) {
                                result['website_links'] = websiteLinks.map(a => ({
                                    url: a.href,
                                    text: a.textContent.trim()
                                }));
                            }
                            
                            // Look for neighborhood links
                            if (key === 'neighborhoods' || term.classList.contains('neighborhoods')) {
                                const neighborhoodLinks = definition.querySelectorAll('a');
                                if (neighborhoodLinks.length) {
                                    result['neighborhoods'] = Array.from(neighborhoodLinks)
                                        .map(a => a.textContent.trim())
                                        .join(', ');
                                }
                            }
                            
                            // Look for category links
                            if (key === 'categories' || term.classList.contains('categories')) {
                                const categoryLinks = definition.querySelectorAll('a');
                                if (categoryLinks.length) {
                                    result['categories'] = Array.from(categoryLinks)
                                        .map(a => a.textContent.trim())
                                        .join(', ');
                                }
                            }
                        }
                    }
                    
                    // Process "Other Information" section to extract nested data
                    const otherInfoTerm = Array.from(container.querySelectorAll('dt')).find(
                        dt => dt.textContent.trim().toLowerCase().includes('other information')
                    );
                    
                    if (otherInfoTerm) {
                        const otherInfoContent = otherInfoTerm.nextElementSibling?.textContent.trim();
                        if (otherInfoContent) {
                            // Extract cuisines
                            const cuisineMatch = otherInfoContent.match(/Cuisines\\s*:\\s*([^\\n.]+)/i);
                            if (cuisineMatch && cuisineMatch[1]) {
                                result['cuisine'] = cuisineMatch[1].trim();
                            }
                            
                            // Extract price range description (only if not conflicting)
                            const priceMatch = otherInfoContent.match(/Price Range\\s*:\\s*([^\\n.]+)/i);
                            if (priceMatch && priceMatch[1]) {
                                result['price_range_description'] = priceMatch[1].trim();
                            }
                            
                            // Check for other patterns we might want to extract
                            const specialtyMatch = otherInfoContent.match(/Specialties\\s*:\\s*([^\\n.]+)/i);
                            if (specialtyMatch && specialtyMatch[1]) {
                                result['specialties'] = specialtyMatch[1].trim();
                            }
                        }
                    }
                }
                
                return Object.keys(result).length > 0 ? result : null;
            }""")
            
            if more_info:
                # Process and clean up the extracted information with deduplication logic
                for key, value in more_info.items():
                    # Skip empty values
                    if not value:
                        continue
                        
                    # Normalize key names
                    normalized_key = key.lower().replace(' ', '_')
                    
                    # Special handling for certain fields
                    if 'payment' in normalized_key:
                        result['payment_methods'] = value
                    elif 'email' in normalized_key:
                        result['email'] = value
                    elif 'hours' in normalized_key or 'opening' in normalized_key:
                        result['hours'] = value
                    elif normalized_key == 'price_range':
                        # Direct price_range field has priority
                        result['price_range'] = value
                    elif normalized_key == 'price_range_description':
                        # Only add description if it's different from the main price_range
                        if value != result.get('price_range'):
                            result['price_range_description'] = value
                    elif normalized_key == 'website_links':
                        # Process website links to extract the main website
                        if isinstance(value, list):
                            for site in value:
                                if isinstance(site, dict) and site.get('url'):
                                    url = site['url']
                                    if not url.startswith('https://www.yellowpages.com'):
                                        result['website'] = url
                                        break
                    else:
                        # For other fields, just use the normalized key
                        result[normalized_key] = value
                
                # Special handling for review data to avoid duplication
                if 'reviews' in result and 'reviews' in more_info:
                    # Keep only one set of reviews (prefer the more detailed one)
                    if isinstance(more_info['reviews'], list) and len(more_info['reviews']) > 0:
                        result['reviews'] = more_info['reviews']
        
        except Exception as e:
            logger.error(f"Error scraping more info section: {str(e)}")
            
        return result
        
    async def extract_special_fields(self, page: Page) -> Dict:
        """Extract special fields like email, social media, etc."""
        result = {}
        
        try:
            # Look for social media links
            social_media = await page.evaluate("""() => {
                const socialLinks = [];
                const socialPlatforms = [
                    { name: 'facebook', patterns: ['facebook.com', 'fb.com'] },
                    { name: 'twitter', patterns: ['twitter.com', 'x.com'] },
                    { name: 'instagram', patterns: ['instagram.com'] },
                    { name: 'linkedin', patterns: ['linkedin.com'] },
                    { name: 'youtube', patterns: ['youtube.com'] },
                    { name: 'yelp', patterns: ['yelp.com'] },
                    { name: 'pinterest', patterns: ['pinterest.com'] },
                    { name: 'tiktok', patterns: ['tiktok.com'] }
                ];
                
                // Gather all links in the document
                const links = Array.from(document.querySelectorAll('a[href]'));
                
                for (const link of links) {
                    const href = link.href.toLowerCase();
                    
                    // Skip empty, javascript, mailto, tel links
                    if (!href || href.startsWith('javascript:') || href.startsWith('mailto:') || href.startsWith('tel:')) {
                        continue;
                    }
                    
                    // Check if this is a social media link
                    for (const platform of socialPlatforms) {
                        if (platform.patterns.some(pattern => href.includes(pattern))) {
                            socialLinks.push({
                                platform: platform.name,
                                url: link.href
                            });
                            break;
                        }
                    }
                }
                
                return socialLinks.length > 0 ? socialLinks : null;
            }""")
            
            if social_media:
                result['social_media'] = social_media
            
            # Extract email address more aggressively
            email = await page.evaluate("""() => {
                // Regular expression for emails
                const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/gi;
                
                // Check mailto links first (most reliable)
                const mailtoLinks = Array.from(document.querySelectorAll('a[href^="mailto:"]'));
                if (mailtoLinks.length > 0) {
                    return mailtoLinks[0].href.replace('mailto:', '').trim();
                }
                
                // Check for visible email addresses in text
                const bodyText = document.body.innerText;
                const emailMatches = bodyText.match(emailRegex);
                
                if (emailMatches && emailMatches.length > 0) {
                    // Filter out common false positives
                    const validEmails = emailMatches.filter(email => 
                        !email.includes('example.com') && 
                        !email.includes('domain.com') &&
                        !email.includes('yourdomain') &&
                        !email.includes('emailaddress') &&
                        !email.includes('email@')
                    );
                    
                    if (validEmails.length > 0) {
                        return validEmails[0];
                    }
                }
                
                // Look for elements with data attributes that might contain email
                const elements = document.querySelectorAll('[data-email], [data-contact-email], [data-business-email]');
                for (const el of elements) {
                    const dataEmail = el.getAttribute('data-email') || 
                                    el.getAttribute('data-contact-email') || 
                                    el.getAttribute('data-business-email');
                    if (dataEmail && emailRegex.test(dataEmail)) {
                        return dataEmail;
                    }
                }
                
                return null;
            }""")
            
            if email:
                result['email'] = email
            
            # Extract business hours in a structured format
            hours = await page.evaluate("""() => {
                try {
                    // Look for hours in structured data first
                    const hoursData = {};
                    
                    // Check for microdata
                    const openingHoursElements = document.querySelectorAll('[itemprop="openingHours"]');
                    if (openingHoursElements.length > 0) {
                        const hours = Array.from(openingHoursElements).map(el => {
                            // Get content attribute or text content
                            return el.getAttribute('content') || el.textContent.trim();
                        });
                        
                        if (hours.length > 0) {
                            return hours;
                        }
                    }
                    
                    // Try to find the hours table or container
                    const hoursContainers = [
                        document.querySelector('.hours-table'),
                        document.querySelector('.business-hours'),
                        document.querySelector('.hours'),
                        document.querySelector('[data-hours]'),
                        document.querySelector('.schedule'),
                        // Look for definition list with hours
                        Array.from(document.querySelectorAll('dt')).find(dt => 
                            dt.textContent.trim().toLowerCase().includes('hour') || 
                            dt.textContent.trim().toLowerCase().includes('open'))?.nextElementSibling
                    ].filter(Boolean);
                    
                    if (hoursContainers.length > 0) {
                        const container = hoursContainers[0];
                        
                        // If this is a table, process it differently
                        if (container.tagName === 'TABLE') {
                            const rows = container.querySelectorAll('tr');
                            const hours = [];
                            
                            for (const row of rows) {
                                const dayCell = row.querySelector('th') || row.cells[0];
                                const hoursCell = row.querySelector('td:last-child') || row.cells[1];
                                
                                if (dayCell && hoursCell) {
                                    hours.push(`${dayCell.textContent.trim()}: ${hoursCell.textContent.trim()}`);
                                }
                            }
                            
                            return hours.length > 0 ? hours : null;
                        }
                        
                        // For non-table containers, try to extract in other ways
                        const text = container.textContent.trim();
                        
                        // If the text contains day abbreviations, try to parse it
                        const days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
                        let foundDays = 0;
                        
                        for (const day of days) {
                            if (text.toLowerCase().includes(day)) {
                                foundDays++;
                            }
                        }
                        
                        // If we find multiple days, this is likely a hours section
                        if (foundDays >= 3) {
                            // Split by line breaks or check for day patterns
                            const lines = text.split(/\\n|<br>/).filter(line => line.trim().length > 0);
                            
                            if (lines.length >= 3) {
                                return lines;
                            } else {
                                // Try to parse from a single string
                                const dayRegex = /(mon|tue|wed|thu|fri|sat|sun)[^:]*:([^\\n]+)/gi;
                                const matches = [];
                                let match;
                                
                                while (match = dayRegex.exec(text)) {
                                    matches.push(`${match[1]}: ${match[2].trim()}`);
                                }
                                
                                if (matches.length > 0) {
                                    return matches;
                                }
                            }
                        }
                    }
                    
                    return null;
                } catch (e) {
                    console.error('Error extracting hours:', e);
                    return null;
                }
            }""")
            
            if hours:
                if isinstance(hours, list):
                    result['hours'] = hours
                else:
                    result['hours'] = str(hours)
                    
        except Exception as e:
            logger.error(f"Error extracting special fields: {str(e)}")
            
        return result
        
    async def scrape_additional_details(self, page: Page) -> Dict:
        """Scrape additional details that might be available but not in structured data"""
        result = {}
        
        try:
            # Extract amenities, features, services, etc.
            additional_details = await page.evaluate("""() => {
                const details = {};
                
                // Look for amenities sections
                const amenitiesSections = [
                    document.querySelector('.amenities'),
                    document.querySelector('.features'),
                    document.querySelector('.services'),
                    // Look for definition terms that mention amenities
                    Array.from(document.querySelectorAll('dt')).find(dt => 
                        dt.textContent.trim().toLowerCase().includes('amenities') || 
                        dt.textContent.trim().toLowerCase().includes('features'))?.nextElementSibling
                ].filter(Boolean);
                
                if (amenitiesSections.length > 0) {
                    const section = amenitiesSections[0];
                    
                    // Check if it's a list
                    const listItems = section.querySelectorAll('li');
                    if (listItems.length > 0) {
                        details.amenities = Array.from(listItems).map(li => li.textContent.trim()).join(', ');
                    } else {
                        // Otherwise just get the text
                        details.amenities = section.textContent.trim();
                    }
                }
                
                // Look for services
                const servicesSections = [
                    document.querySelector('.services'),
                    document.querySelector('.service-list'),
                    // Look for definition terms that mention services
                    Array.from(document.querySelectorAll('dt')).find(dt => 
                        dt.textContent.trim().toLowerCase().includes('services'))?.nextElementSibling
                ].filter(Boolean);
                
                if (servicesSections.length > 0) {
                    const section = servicesSections[0];
                    
                    // Check if it's a list
                    const listItems = section.querySelectorAll('li');
                    if (listItems.length > 0) {
                        details.services = Array.from(listItems).map(li => li.textContent.trim()).join(', ');
                    } else {
                        // Otherwise just get the text
                        details.services = section.textContent.trim();
                    }
                }
                
                // Look for specialty or expertise
                const specialtySections = [
                    document.querySelector('.specialties'),
                    document.querySelector('.expertise'),
                    // Look for definition terms that mention specialties
                    Array.from(document.querySelectorAll('dt')).find(dt => 
                        dt.textContent.trim().toLowerCase().includes('special') || 
                        dt.textContent.trim().toLowerCase().includes('expert'))?.nextElementSibling
                ].filter(Boolean);
                
                if (specialtySections.length > 0) {
                    const section = specialtySections[0];
                    
                    // Check if it's a list
                    const listItems = section.querySelectorAll('li');
                    if (listItems.length > 0) {
                        details.specialties = Array.from(listItems).map(li => li.textContent.trim()).join(', ');
                    } else {
                        // Otherwise just get the text
                        details.specialties = section.textContent.trim();
                    }
                }
                
                // Look for brands, products, or offerings
                const brandsSections = [
                    document.querySelector('.brands'),
                    document.querySelector('.products'),
                    document.querySelector('.offerings'),
                    // Look for definition terms that mention brands
                    Array.from(document.querySelectorAll('dt')).find(dt => 
                        dt.textContent.trim().toLowerCase().includes('brand') || 
                        dt.textContent.trim().toLowerCase().includes('product') ||
                        dt.textContent.trim().toLowerCase().includes('offer'))?.nextElementSibling
                ].filter(Boolean);
                
                if (brandsSections.length > 0) {
                    const section = brandsSections[0];
                    
                    // Check if it's a list
                    const listItems = section.querySelectorAll('li');
                    if (listItems.length > 0) {
                        details.brands = Array.from(listItems).map(li => li.textContent.trim()).join(', ');
                    } else {
                        // Otherwise just get the text
                        details.brands = section.textContent.trim();
                    }
                }
                
                return Object.keys(details).length > 0 ? details : null;
            }""")
            
            if additional_details:
                # Add non-duplicate fields
                for key, value in additional_details.items():
                    # Only add if not already present in result
                    if key not in result:
                        result[key] = value
                
        except Exception as e:
            logger.error(f"Error scraping additional details: {str(e)}")
            
        return result

    async def run(self):
        """Execute the scraping workflow"""
        try:
            async with async_playwright() as p:
                # Configure browser launch with enhanced stealth
                self.browser = await p.chromium.launch(
                    headless=True,  # Set to True for production
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--start-maximized',
                        '--disable-extensions',
                        '--disable-popup-blocking',
                        '--disable-infobars',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-accelerated-2d-canvas',
                        '--no-first-run',
                        '--no-default-browser-check',
                        '--disable-gpu',
                        '--disable-notifications',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-breakpad',
                        '--disable-component-extensions-with-background-pages',
                        '--disable-features=TranslateUI,BlinkGenPropertyTrees',
                        '--disable-ipc-flooding-protection',
                        '--disable-renderer-backgrounding',
                        '--mute-audio',
                        '--hide-scrollbars',
                    ],
                    slow_mo=random.randint(50, 150)  # More moderate slowdown
                )
                
                # Execute scraping steps
                await self.gather_listing_urls()
                
                if self.listing_urls:
                    logger.info(f"Collected {len(self.listing_urls)} listings")
                    await self.scrape_restaurant_listings()
                else:
                    logger.error("No listings collected - check selectors")
                
                self.save_results()
                await self.browser.close()
        
        except Exception as e:
            logger.critical(f"Scraping failed: {str(e)}")
            if self.browser:
                await self.browser.close()
            raise

    def throttle_request(self):
        """Implement request throttling with variable delays"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            delay = self.min_request_interval + random.uniform(0, self.max_request_interval - self.min_request_interval)
            time.sleep(delay)
            
        self.last_request_time = time.time()

    async def gather_listing_urls(self):
        """Collect listing URLs from search pages"""
        for page_num in range(1, self.page_limit + 1):
            try:
                # Throttle requests
                self.throttle_request()
                
                # Get browser context with rotating UA and optional proxy
                context = await self.get_stealth_context()
                
                page = await context.new_page()
                
                try:
                    # Apply browser fingerprint evasion
                    await self.apply_stealth_techniques(page)
                    
                    url = f"{self.search_url}?page={page_num}" if page_num > 1 else self.search_url
                    logger.info(f"Processing page {page_num}...")
                    
                    # More human-like navigation pattern
                    await self.human_like_navigation(page, url, page_num)
                    
                    # Wait for the exact results container
                    await self.wait_for_results_container(page)
                    
                    # Human-like scrolling before extraction
                    await self.human_like_scrolling(page)
                    
                    # Extract links with precise targeting
                    links = await self.extract_links_with_precision(page, page_num)
                    
                    if links:
                        self.listing_urls.extend(links)
                        logger.info(f"Extracted {len(links)} links from page {page_num}")
                    else:
                        logger.warning(f"No links found on page {page_num}")
                        await self.debug_page(page, f"no_links_page_{page_num}")
                    
                    # Variable delay between pages
                    if page_num < self.page_limit:
                        delay = random.uniform(self.search_page_delay, self.search_page_delay + 5)
                        logger.info(f"Waiting {delay:.2f} seconds before next page...")
                        await asyncio.sleep(delay)
                
                finally:
                    await page.close()
                    await context.close()
            
            except Exception as e:
                logger.error(f"Page {page_num} failed: {str(e)}")
                # Wait longer after an error
                await asyncio.sleep(10 + random.random() * 5)
                continue

    async def get_stealth_context(self) -> BrowserContext:
        """Create a new browser context with anti-detection measures"""
        user_agent = random.choice(self.user_agents)
        
        # Randomize viewport dimensions slightly
        width = random.choice([1366, 1440, 1536, 1600, 1920])
        height = random.choice([768, 800, 864, 900, 1080])
        
        context_options = {
            "user_agent": user_agent,
            "viewport": {'width': width, 'height': height},
            "java_script_enabled": True,
            "locale": random.choice(['en-US', 'en-GB', 'en-CA']),
            "timezone_id": random.choice(['America/Los_Angeles', 'America/New_York', 'America/Chicago']),
            "geolocation": {"latitude": 34.0522, "longitude": -118.2437, "accuracy": 100},  # LA coordinates
            "permissions": ["geolocation"],
            "color_scheme": random.choice(["light", "dark", "no-preference"]),
            "device_scale_factor": random.choice([1, 1.25, 1.5, 2]),
            "is_mobile": False,
            "has_touch": random.choice([True, False]),
            "reduced_motion": random.choice(["reduce", "no-preference"]),
            "accept_downloads": True,
        }
        
        # Add proxy if available
        if self.proxies:
            proxy = random.choice(self.proxies)
            context_options["proxy"] = {"server": proxy}
        
        return await self.browser.new_context(**context_options)

    async def apply_stealth_techniques(self, page: Page):
        """Apply various stealth techniques to the page"""
        # Override navigator properties to prevent fingerprinting
        await page.add_init_script("""
        () => {
            // Overwrite the navigator properties
            const overrides = {
                webdriver: false,
                chrome: { runtime: {} },
                languages: ["en-US", "en"],
                plugins: {
                    length: Math.floor(Math.random() * 5) + 3,
                    item: () => null,
                    namedItem: () => null,
                    refresh: () => {},
                }
            };
            
            // Apply the overrides to prevent detection
            Object.defineProperties(navigator, {
                webdriver: {
                    get: () => overrides.webdriver,
                    configurable: true
                }
            });
            
            // Randomize navigator values slightly
            const navProto = Navigator.prototype;
            for (const prop in overrides) {
                if (prop in navProto) {
                    Object.defineProperty(navProto, prop, {
                        get: () => overrides[prop]
                    });
                }
            }
            
            // Hide automation flags
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            
            // Random window dimensions slight offset to avoid perfect values
            Object.defineProperty(window, 'innerWidth', {
                get: () => window.outerWidth - Math.floor(Math.random() * 10) - 10
            });
            Object.defineProperty(window, 'innerHeight', {
                get: () => window.outerHeight - Math.floor(Math.random() * 10) - 90
            });
            
            // Add random plugin data
            const mimeTypes = [
                'application/pdf',
                'application/x-google-chrome-pdf',
                'application/x-nacl',
                'application/x-pnacl',
                'application/x-shockwave-flash'
            ];
            
            // Fake canvas fingerprint
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type) {
                if (type === 'image/png' && this.width === 16 && this.height === 16) {
                    return originalToDataURL.apply(this, arguments);
                }
                return originalToDataURL.apply(this, arguments);
            };
        }
        """)

    async def human_like_navigation(self, page: Page, url: str, page_num: int):
        """Navigate with a more human-like pattern"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                # First visit an intermediate page
                if page_num == 1 or random.random() < 0.5:
                    intermediate_sites = [
                        "https://www.google.com/search?q=best+restaurants+in+los+angeles",
                        "https://www.google.com/search?q=yellowpages+restaurants+reviews",
                        "https://www.bing.com/search?q=restaurant+directory+california",
                        "https://www.tripadvisor.com/Restaurants-g32655-Los_Angeles_California.html"
                    ]
                    
                    intermediate_site = random.choice(intermediate_sites)
                    logger.info(f"Visiting intermediate site: {intermediate_site}")
                    
                    await page.goto(
                        intermediate_site,
                        wait_until="domcontentloaded",
                        timeout=self.timeout
                    )
                    
                    # Do some random scrolling
                    await self.human_like_scrolling(page, scroll_count=random.randint(1, 3))
                    
                    # Wait with random time after visiting intermediate site
                    await asyncio.sleep(2 + random.random() * 2)
                
                # Main navigation to target URL
                logger.info(f"Navigating to target URL: {url}")
                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.timeout
                )
                
                await asyncio.sleep(2 + random.random() * 2)
                
                # Perform random mouse movements
                await self.random_mouse_movements(page)
                
                # Check for blocks or captchas
                if await self.detect_blocking(page):
                    if attempt < max_attempts:
                        logger.warning(f"Block detected, attempt {attempt}/{max_attempts}. Waiting before retry...")
                        # Longer wait after block detection
                        await asyncio.sleep(10 + attempt * 5 + random.random() * 10)
                        continue
                    else:
                        raise Exception("Site blocked access after multiple attempts")
                
                return
            
            except Exception as e:
                if attempt == max_attempts:
                    raise
                logger.warning(f"Navigation attempt {attempt} failed, retrying: {str(e)}")
                await asyncio.sleep(5 * attempt + random.random() * 5)

    async def detect_blocking(self, page: Page) -> bool:
        """Detect if the site is blocking us"""
        block_indicators = [
            'text="Access Denied"',
            'text="Captcha"',
            'text="Security Check"',
            'text="We noticed unusual activity"',
            'text="Please confirm you are a human"',
            'text="Your IP has been blocked"',
            'text="Forbidden"',
            'text="Too many requests"',
            'input[name="captcha"]',
            '#captcha',
            '.captcha',
            'iframe[src*="captcha"]',
            'iframe[src*="recaptcha"]',
            '.g-recaptcha'
        ]
        
        for selector in block_indicators:
            try:
                if await page.query_selector(selector):
                    logger.warning(f"Block indicator found: {selector}")
                    return True
            except:
                pass
        
        # Check page title for blocking indications
        title = await page.title()
        block_title_keywords = ["blocked", "denied", "captcha", "security", "forbidden", "unusual activity"]
        if any(keyword in title.lower() for keyword in block_title_keywords):
            logger.warning(f"Block indicator found in title: {title}")
            return True
            
        return False

    async def random_mouse_movements(self, page: Page):
        """Perform random mouse movements to mimic human behavior"""
        width = await page.evaluate('window.innerWidth')
        height = await page.evaluate('window.innerHeight')
        
        # Generate random points
        points = []
        for _ in range(random.randint(3, 8)):
            points.append({
                'x': random.randint(0, width),
                'y': random.randint(0, height)
            })
        
        # Move mouse through these points
        for point in points:
            await page.mouse.move(point['x'], point['y'], steps=random.randint(5, 15))
            await asyncio.sleep(random.random() * 0.5)
            
        # Random clicks (not on specific elements to avoid unintended actions)
        if random.random() < 0.3:  # 30% chance
            # Click in a safe area (usually the top quarter of the page)
            safe_x = random.randint(width // 4, 3 * width // 4)
            safe_y = random.randint(height // 10, height // 4)
            
            await page.mouse.move(safe_x, safe_y, steps=random.randint(5, 10))
            await asyncio.sleep(random.random() * 0.3)
            await page.mouse.down()
            await asyncio.sleep(random.random() * 0.1)
            await page.mouse.up()

    async def human_like_scrolling(self, page: Page, scroll_count=None):
        """Simulate human-like scrolling behavior"""
        if scroll_count is None:
            scroll_count = random.randint(3, 7)
        
        height = await page.evaluate('document.body.scrollHeight')
        viewport_height = await page.evaluate('window.innerHeight')
        
        # Randomize scroll positions and speeds
        for i in range(scroll_count):
            # Calculate a random scroll position, weighted more towards middle of page
            scroll_position = random.triangular(
                viewport_height * (i / scroll_count),
                min(height, viewport_height * ((i + 1.5) / scroll_count)),
                min(height, viewport_height * ((i + 1) / scroll_count))
            )
            
            # Scroll with variable speed
            await page.evaluate(f'window.scrollTo({{top: {scroll_position}, behavior: "smooth"}})')
            
            # Wait a random amount of time between scrolls to mimic reading
            await asyncio.sleep(random.uniform(0.5, 2.5))
            
            # Sometimes pause longer as if reading content
            if random.random() < 0.3:  # 30% chance to pause longer
                await asyncio.sleep(random.uniform(1.0, 4.0))
                
            # Occasionally wiggle the scroll position slightly
            if random.random() < 0.4:  # 40% chance to wiggle
                wiggle = random.uniform(-100, 100)
                await page.evaluate(f'window.scrollBy(0, {wiggle})')
                await asyncio.sleep(random.uniform(0.3, 0.7))

    async def wait_for_results_container(self, page: Page):
        """Precisely wait for the correct results container"""
        container_selector = 'div.search-results.organic:not(.center-ads)'
        
        try:
            await page.wait_for_selector(
                container_selector,
                state="attached",
                timeout=self.timeout
            )
            
            # Additional check for actual listings
            await page.wait_for_selector(
                f'{container_selector} .result, {container_selector} .listing',
                timeout=self.timeout
            )
            
            logger.debug("Correct results container found")
        except Exception as e:
            logger.error(f"Failed to find results container: {str(e)}")
            await self.debug_page(page, "missing_container")
            raise

    async def extract_links_with_precision(self, page: Page, page_num: int) -> List[Dict[str, str]]:
        """Extract links with exact targeting of the organic results"""
        try:
            # Wait briefly to ensure page is fully loaded
            await asyncio.sleep(1 + random.random())
            
            return await page.evaluate("""() => {
                const results = [];
                const container = document.querySelector('div.search-results.organic:not(.center-ads)');
                
                if (!container) {
                    console.error('Organic results container not found');
                    return [];
                }
                
                // Try different possible selectors for listings
                const listings = container.querySelectorAll('.result, .listing, .business-listing');
                
                listings.forEach((listing, index) => {
                    try {
                        // Try different possible selectors for business names/links
                        const link = listing.querySelector('a.business-name, .business-link, .name a, h2 a');
                        
                        if (link && link.href) {
                            // Make sure we're getting detail pages
                            if (link.href.includes('/mip/') || link.href.includes('/business/')) {
                                results.push({
                                    url: link.href,
                                    title: link.textContent.trim() || 'Untitled',
                                    position: index + 1
                                });
                            }
                        }
                    } catch (e) {
                        console.warn('Error processing listing:', e);
                    }
                });
                
                return results;
            }""")
        except Exception as e:
            logger.error(f"Link extraction failed on page {page_num}: {str(e)}")
            return []

    async def debug_page(self, page: Page, debug_name: str):
        """Capture debugging information"""
        debug_dir = self.output_dir / "debug"
        debug_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%H%M%S')
        await page.screenshot(path=debug_dir / f"{debug_name}_{timestamp}.png", full_page=True)
        html = await page.content()
        with open(debug_dir / f"{debug_name}_{timestamp}.html", 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"Debug files saved for {debug_name}")

    async def scrape_restaurant_listings(self):
        """Scrape listings with batched concurrency and delay distribution"""
        # Randomly shuffle the listing URLs to make the access pattern less predictable
        random.shuffle(self.listing_urls)
        
        link_batches = [
            self.listing_urls[i:i + self.links_per_agent] 
            for i in range(0, len(self.listing_urls), self.links_per_agent)
        ]
        
        # Use a smaller batch size for more randomness in access patterns
        reduced_batch_size = max(1, min(self.batch_size, len(link_batches) // 2))
        semaphore = asyncio.Semaphore(reduced_batch_size)
        tasks = []
        
        for batch_num, batch_links in enumerate(link_batches, 1):
            # Add jitter to task scheduling
            await asyncio.sleep(random.random() * 2)
            task = self.process_batch(batch_links, batch_num, semaphore)
            tasks.append(task)
        
        # Process with random deltas
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process any exceptions
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Batch processing error: {str(result)}")

    async def process_batch(self, links_batch: List[Dict[str, str]], batch_num: int, semaphore: asyncio.Semaphore):
        """Process a batch of links with fresh context"""
        async with semaphore:
            # Add jitter to batch processing
            await asyncio.sleep(random.uniform(1, 5))
            
            # Create a new context for each batch
            context = await self.get_stealth_context()
            
            try:
                # Randomize the sequence within the batch
                random.shuffle(links_batch)
                
                for link_num, link in enumerate(links_batch, 1):
                    try:
                        self.throttle_request()
                        await self.scrape_single_listing(context, link, batch_num, link_num)
                        
                        # Variable delay between listings
                        delay = random.uniform(3, 8)
                        logger.info(f"Waiting {delay:.2f} seconds before next listing...")
                        await asyncio.sleep(delay)
                    except Exception as e:
                        logger.error(f"Batch {batch_num}-{link_num} failed: {str(e)}")
                        # Longer wait after an error
                        await asyncio.sleep(5 + random.random() * 10)
                        continue
            finally:
                await context.close()

    async def scrape_single_listing(self, context: BrowserContext, link: Dict[str, str], batch_num: int, link_num: int):
        """Scrape individual listing page with improved reliability"""
        page = await context.new_page()
        
        try:
            # Apply stealth techniques to each page
            await self.apply_stealth_techniques(page)
            
            logger.info(f"[Batch {batch_num}-{link_num}] Processing: {link['title'][:50]}...")
            
            # Use human-like navigation
            await self.human_like_navigation(page, link['url'], 0)
            
            # Wait with random timing
            await asyncio.sleep(1 + random.random() * 2)
            
            # Wait for content to load
            try:
                # Try multiple possible selectors for key content elements
                await page.wait_for_selector([
                    '.business-card', 
                    '.mip-header__info',
                    'script[type="application/ld+json"]',
                    '.business-info',
                    'h1',
                    '.address',
                    '.phone',
                    '.contact-info'
                ].join(', '), timeout=20000, state='attached')
            except Exception as e:
                logger.warning(f"Could not find primary selectors, but continuing: {str(e)}")
                # Continue anyway - we'll extract what we can
            
            # Wait for the page to be fully loaded
            await page.wait_for_load_state('networkidle', timeout=15000)
            
            # Random interactions before extraction
            await self.random_mouse_movements(page)
            await self.human_like_scrolling(page, scroll_count=random.randint(2, 4))
            
            # Extract the data with our improved methods
            data = await self.extract_listing_data(page, link['url'])
            
            # Make sure we have at least a name before saving
            if data and data.get('name'):
                self.results.append(data)
            else:
                logger.warning(f"Extracted data had no name for {link['url']}")
                # Still add to results with at least the URL
                data['name'] = link['title']  # Use the link title as a fallback
                self.results.append(data)
        
        except Exception as e:
            logger.error(f"Failed to scrape {link['url']}: {str(e)}")
            await self.debug_page(page, f"failed_{batch_num}_{link_num}")
            
            # Still try to add basic info to results
            self.results.append({
                "name": link.get('title', 'Unknown'),
                "listing_url": link['url'],
                "scraped_at": datetime.now().isoformat(),
                "source": "yellowpages",
                "scrape_error": str(e)
            })
        finally:
            await page.close()

    async def extract_json_ld(self, page: Page) -> Optional[Dict]:
        """Extract JSON-LD data with enhanced error handling"""
        try:
            # Try multiple ways to get JSON-LD
            return await page.evaluate("""() => {
                try {
                    // Look for script tags with application/ld+json
                    const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                    
                    if (scripts && scripts.length) {
                        for (const script of scripts) {
                            try {
                                const content = script.textContent.trim();
                                if (content.includes('Restaurant') || 
                                    content.includes('LocalBusiness') || 
                                    content.includes('Food') ||
                                    content.includes('rating') ||
                                    content.includes('address')) {
                                    return JSON.parse(content);
                                }
                            } catch (parseError) {
                                console.error('Error parsing JSON-LD:', parseError);
                            }
                        }
                        
                        // If no matching criteria but we have scripts, return the first one
                        if (scripts.length > 0) {
                            try {
                                return JSON.parse(scripts[0].textContent.trim());
                            } catch (e) {}
                        }
                    }
                    
                    return null;
                } catch (e) {
                    console.error('Error in JSON-LD extraction:', e);
                    return null;
                }
            }""")
        except Exception as e:
            logger.error(f"JSON-LD extraction error: {str(e)}")
            return None

    async def extract_listing_data(self, page: Page, url: str) -> Dict[str, Any]:
        """Extract all available data from listing page"""
        data = {
            "listing_url": url,
            "scraped_at": datetime.now().isoformat(),
            "source": "yellowpages"
        }
        
        try:
            # Wait for the page to be fully loaded
            await page.wait_for_load_state('networkidle', timeout=20000)
            
            # First try to extract JSON-LD data (more reliable and complete)
            json_ld = await self.extract_json_ld(page)
            if json_ld:
                logger.info("JSON-LD data found, extracting...")
                data.update(self.parse_json_ld(json_ld))
                
                # Even with JSON-LD, scrape additional fields that might not be in the structured data
                extra_data = await self.scrape_additional_details(page)
                # Only add non-duplicate fields
                for key, value in extra_data.items():
                    if key not in data:
                        data[key] = value
            else:
                # Fallback to direct scraping if no JSON-LD
                logger.info("No JSON-LD found, falling back to direct scraping...")
                direct_data = await self.direct_scrape_fallback(page)
                data.update(direct_data)
            
            # Always try to scrape the full info section which contains rich details
            more_info = await self.scrape_more_info_section(page)
            if more_info:
                # Carefully merge more_info with existing data to avoid duplicates
                for key, value in more_info.items():
                    # Special handling for price_range to prevent duplication
                    if key == 'price_range':
                        if 'price_range' not in data:
                            data['price_range'] = value
                    # For other fields, prefer existing data but add new fields
                    elif key not in data:
                        data[key] = value
                    # Special case for website if we don't have one yet
                    elif key == 'website' and not data.get('website'):
                        data['website'] = value
            
            # Special fields to look for in any case
            special_data = await self.extract_special_fields(page)
            # Merge without overwriting existing fields
            for key, value in special_data.items():
                if key not in data:
                    data[key] = value
            
            # If we still don't have a name, try one last approach
            if not data.get('name'):
                title = await page.title()
                if title:
                    # Extract business name from title (usually "Business Name - YP.com")
                    name_match = re.match(r'^([^-]+)', title)
                    if name_match:
                        data['name'] = name_match.group(1).strip()
                    else:
                        data['name'] = title.split('|')[0].strip()
            
            # Ensure proper data formatting
            self.standardize_output_format(data)
            
            logger.info(f"Extracted data for: {data.get('name', 'Unknown business')}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error extracting listing data: {str(e)}")
            
            # Even if there was an error, return whatever data we have
            if not data.get('name'):
                title = await page.title()
                if title:
                    data['name'] = title.split('|')[0].strip()
            
            return data

    def standardize_output_format(self, data: Dict):
        """Standardize and clean the output data format to avoid duplicates"""
        # Normalize phone numbers
        if data.get('phone'):
            phone = data['phone']
            # Strip all non-numeric characters
            digits = re.sub(r'\D', '', phone)
            if len(digits) == 10:
                data['phone'] = f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
            elif len(digits) == 11 and digits[0] == '1':
                data['phone'] = f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
        
        # Clean website URLs
        if data.get('website'):
            website = data['website']
            # Remove tracking parameters
            if '?' in website and ('utm_' in website or 'y_source=' in website):
                data['website'] = website.split('?')[0]
            # Ensure https if missing protocol
            if not website.startswith('http'):
                data['website'] = 'https://' + website
        
        # Consolidate categories and cuisines
        if data.get('categories') and data.get('cuisine'):
            # If they are identical, remove the duplicated field
            if data['categories'].lower() == data['cuisine'].lower():
                del data['cuisine']
        
        # Handle price range properly
        if 'price_range_description' in data and 'price_range' not in data:
            data['price_range'] = data['price_range_description']
            del data['price_range_description']

    def parse_json_ld(self, data: Dict) -> Dict:
        """Parse JSON-LD into our standardized format with enhanced detail extraction"""
        result = {}
        
        # Handle array of JSON-LD objects
        if isinstance(data, list):
            # Find the most relevant object (Restaurant or LocalBusiness)
            for item in data:
                if isinstance(item, dict):
                    type_value = item.get('@type', '')
                    if isinstance(type_value, str) and ('Restaurant' in type_value or 'LocalBusiness' in type_value):
                        data = item
                        break
            # If no restaurant/business found, use the first item
            if isinstance(data, list) and data:
                data = data[0]
        
        if not isinstance(data, dict):
            return result
        
        # Standard fields with expanded mapping
        mapping = {
            'name': ['name'],
            'phone': ['telephone', 'phone'],
            'website': ['url', 'website', 'sameAs'],
            'price_range': ['priceRange'],
            'description': ['description', 'about'],
            'business_id': ['@id'],
            'image': ['image', 'photo', 'logo'],
            'menu': ['menu', 'hasMenu'],
            'email': ['email'],
            'cuisine': ['servesCuisine'],
            'payment_accepted': ['paymentAccepted'],
        }
        
        # Try each possible field name
        for our_field, ld_fields in mapping.items():
            for ld_field in ld_fields:
                if data.get(ld_field):
                    value = data[ld_field]
                    
                    # Handle nested objects like image
                    if our_field == 'image' and isinstance(value, dict):
                        if value.get('url'):
                            result[our_field] = value['url']
                        elif value.get('contentUrl'):
                            result[our_field] = value['contentUrl']
                    # Handle arrays for fields like sameAs (for website)
                    elif our_field == 'website' and isinstance(value, list):
                        # Find the first http URL
                        for item in value:
                            if isinstance(item, str) and (item.startswith('http://') or item.startswith('https://')):
                                result[our_field] = item
                                break
                    # Handle other fields directly
                    else:
                        result[our_field] = value
                    
                    # Once we've found a value, stop looking at alternative field names
                    if our_field in result:
                        break
        
        # Address fields - handle both string and object formats
        if data.get('address'):
            address = data['address']
            
            # Handle object format
            if isinstance(address, dict):
                address_mapping = {
                    'street_address': ['streetAddress', 'street'],
                    'city': ['addressLocality', 'city'],
                    'state': ['addressRegion', 'state'],
                    'zip_code': ['postalCode', 'zip', 'zipCode'],
                    'country': ['addressCountry', 'country']
                }
                
                for our_field, ld_fields in address_mapping.items():
                    for ld_field in ld_fields:
                        if address.get(ld_field):
                            result[our_field] = address[ld_field]
                            break
            
            # Handle string format (fall back if needed)
            elif isinstance(address, str):
                parts = address.split(',')
                if len(parts) >= 3:  # Assuming format like "123 Main St, City, State ZIP"
                    result['street_address'] = parts[0].strip()
                    result['city'] = parts[1].strip()
                    
                    # Handle "State ZIP" format in the last part
                    state_zip = parts[2].strip().split(' ')
                    if len(state_zip) >= 2:
                        result['state'] = state_zip[0].strip()
                        # Join the rest as ZIP (in case it has a hyphen or extra spaces)
                        result['zip_code'] = ' '.join(state_zip[1:]).strip()
                    else:
                        result['state'] = parts[2].strip()
        
        # Geo coordinates
        if data.get('geo'):
            geo = data['geo']
            if isinstance(geo, dict):
                # Try multiple field names for coordinates
                for lat_field in ['latitude', 'lat']:
                    if geo.get(lat_field) is not None:
                        try:
                            result['latitude'] = float(geo[lat_field])
                        except (ValueError, TypeError):
                            pass
                        break
                
                for lng_field in ['longitude', 'lng', 'long']:
                    if geo.get(lng_field) is not None:
                        try:
                            result['longitude'] = float(geo[lng_field])
                        except (ValueError, TypeError):
                            pass
                        break
        
        # Rating information
        if data.get('aggregateRating'):
            rating = data['aggregateRating']
            if isinstance(rating, dict):
                for field in ['ratingValue', 'rating']:
                    if rating.get(field) is not None:
                        try:
                            result['rating'] = float(rating[field])
                        except (ValueError, TypeError):
                            pass
                        break
                
                for field in ['reviewCount', 'count', 'ratingCount']:
                    if rating.get(field) is not None:
                        try:
                            result['review_count'] = int(rating[field])
                        except (ValueError, TypeError):
                            pass
                        break
        
        # Opening hours
        if data.get('openingHours') or data.get('openingHoursSpecification'):
            hours_data = data.get('openingHours') or data.get('openingHoursSpecification')
            
            if isinstance(hours_data, list):
                # Handle array of hours
                if all(isinstance(h, str) for h in hours_data):
                    # Array of strings like ["Mo-Fr 9:00-17:00", "Sa 10:00-13:00"]
                    result['hours'] = hours_data
                elif all(isinstance(h, dict) for h in hours_data):
                    # Array of objects with day and time info
                    formatted_hours = []
                    for hour in hours_data:
                        day = hour.get('dayOfWeek')
                        if day and isinstance(day, str):
                            day = day.replace('http://schema.org/', '')
                        opens = hour.get('opens')
                        closes = hour.get('closes')
                        
                        if day and opens and closes:
                            formatted_hours.append(f"{day}: {opens}-{closes}")
                        elif day:
                            time_spec = []
                            if opens:
                                time_spec.append(f"opens {opens}")
                            if closes:
                                time_spec.append(f"closes {closes}")
                            if time_spec:
                                formatted_hours.append(f"{day}: {' '.join(time_spec)}")
                    
                    if formatted_hours:
                        result['hours'] = formatted_hours
            elif isinstance(hours_data, str):
                # Single string with hours
                result['hours'] = hours_data
                
        # Reviews
        if data.get('review') and isinstance(data['review'], list):
            reviews = []
            
            for review_data in data['review']:
                if not isinstance(review_data, dict):
                    continue
                    
                review = {}
                
                # Extract author
                if review_data.get('author'):
                    author = review_data['author']
                    if isinstance(author, dict) and author.get('name'):
                        review['author'] = author['name']
                    elif isinstance(author, str):
                        review['author'] = author
                
                # Extract review text
                if review_data.get('reviewBody'):
                    review['text'] = review_data['reviewBody']
                elif review_data.get('description'):
                    review['text'] = review_data['description']
                
                # Extract rating
                if review_data.get('reviewRating'):
                    rating = review_data['reviewRating']
                    if isinstance(rating, dict) and rating.get('ratingValue'):
                        try:
                            review['rating'] = float(rating['ratingValue'])
                        except (ValueError, TypeError):
                            pass
                
                # Extract date
                if review_data.get('datePublished'):
                    review['date'] = review_data['datePublished']
                
                # Only add reviews with at least some content
                if review and (review.get('text') or review.get('author')):
                    reviews.append(review)
            
            if reviews:
                result['reviews'] = reviews
        
        # Special handling for parsing a menu URL
        if data.get('hasMenu'):
            menu = data['hasMenu']
            if isinstance(menu, dict) and menu.get('url'):
                result['menu_url'] = menu['url']
            elif isinstance(menu, str) and (menu.startswith('http://') or menu.startswith('https://')):
                result['menu_url'] = menu
        
        return result

    async def direct_scrape_fallback(self, page: Page) -> Dict:
        """Enhanced direct scraping fallback when JSON-LD is missing"""
        result = {}
        
        # Try multiple approaches to get the name
        name = await page.evaluate("""() => {
            // Try multiple selectors for name
            return (
                document.querySelector('h1')?.textContent.trim() || 
                document.querySelector('.business-name')?.textContent.trim() ||
                document.querySelector('.business-card-container h1')?.textContent.trim() ||
                document.querySelector('.business-info h1')?.textContent.trim() ||
                document.querySelector('h1.business-title')?.textContent.trim() ||
                document.querySelector('.mip-header__info h1')?.textContent.trim() ||
                document.querySelector('[itemprop="name"]')?.textContent.trim() ||
                document.title.split('|')[0]?.trim() ||
                ''
            );
        }""")
        if name:
            result['name'] = name
        
        # Phone extraction with multiple approaches
        phone = await page.evaluate("""() => {
            return (
                document.querySelector('.phone')?.textContent.trim() || 
                document.querySelector('a[href^="tel:"]')?.textContent.trim() ||
                document.querySelector('[itemprop="telephone"]')?.textContent.trim() ||
                document.querySelector('.business-phone')?.textContent.trim() ||
                document.querySelector('.telephone')?.textContent.trim() ||
                document.querySelector('.phone-number')?.textContent.trim() ||
                document.querySelector('.dockable-phone')?.textContent.trim() ||
                ''
            ).replace(/\\D+/g, '').replace(/^1/, '');  // Strip non-digits and leading 1
        }""")
        if phone:
            if len(phone) == 10:  # Ensure it's a valid 10-digit US phone
                result['phone'] = f"({phone[0:3]}) {phone[3:6]}-{phone[6:10]}"
            else:
                result['phone'] = phone
                
        # Email extraction
        email = await page.evaluate("""() => {
            // Look for visible email addresses
            const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/;
            const content = document.body.textContent || '';
            const match = content.match(emailRegex);
            
            // Try finding email in links
            const mailtoLinks = Array.from(document.querySelectorAll('a[href^="mailto:"]'));
            const mailtoEmail = mailtoLinks.length > 0 ? 
                mailtoLinks[0].href.replace('mailto:', '').trim() : null;
            
            return match ? match[0] : mailtoEmail;
        }""")
        if email:
            result['email'] = email
        
        # Address extraction with improved selectors
        address_components = await page.evaluate("""() => {
            // Try multiple container selectors
            const containers = [
                document.querySelector('.address'),
                document.querySelector('.business-card'),
                document.querySelector('.contact'),
                document.querySelector('.contact-info'),
                document.querySelector('.business-address'),
                document.querySelector('[itemprop="address"]'),
                document.querySelector('.address-container')
            ].filter(Boolean);
            
            // If no containers found, try direct approaches
            if (!containers.length) {
                // Look for structured address parts across the page
                return {
                    street: document.querySelector('[itemprop="streetAddress"]')?.textContent.trim(),
                    city: document.querySelector('[itemprop="addressLocality"]')?.textContent.trim(),
                    state: document.querySelector('[itemprop="addressRegion"]')?.textContent.trim(),
                    zip: document.querySelector('[itemprop="postalCode"]')?.textContent.trim(),
                    country: document.querySelector('[itemprop="addressCountry"]')?.textContent.trim()
                };
            }
            
            // Check each container
            for (const container of containers) {
                // Try to get address parts from this container
                const street = container.querySelector('.street-address, [itemprop="streetAddress"]')?.textContent.trim();
                const city = container.querySelector('.locality, [itemprop="addressLocality"]')?.textContent.trim();
                const state = container.querySelector('.region, [itemprop="addressRegion"]')?.textContent.trim();
                const zip = container.querySelector('.postal-code, [itemprop="postalCode"]')?.textContent.trim();
                const country = container.querySelector('[itemprop="addressCountry"]')?.textContent.trim();
                
                // If we found useful data, return it
                if (street || city || state || zip) {
                    return { street, city, state, zip, country };
                }
            }
            
            // As a last resort, try to extract from the full text of a potential address block
            const addressBlock = document.querySelector('.address, .business-address, .address-container');
            if (addressBlock) {
                const fullText = addressBlock.textContent.trim();
                // Simple pattern matching for US addresses
                const parts = fullText.split(',');
                if (parts.length >= 2) {
                    const street = parts[0].trim();
                    const cityRegionMatch = parts[1].trim().match(/([^,]+),?\\s*([A-Z]{2})\\s+(\\d{5}(-\\d{4})?)/);
                    
                    if (cityRegionMatch) {
                        return {
                            street,
                            city: cityRegionMatch[1].trim(),
                            state: cityRegionMatch[2],
                            zip: cityRegionMatch[3]
                        };
                    }
                }
            }
            
            return null;
        }""")
        
        if address_components:
            if address_components.get('street'):
                result['street_address'] = address_components['street']
            if address_components.get('city'):
                result['city'] = address_components['city']
            if address_components.get('state'):
                result['state'] = address_components['state']
            if address_components.get('zip'):
                result['zip_code'] = address_components['zip']
            if address_components.get('country'):
                result['country'] = address_components['country']
        
        # Website URL
        website = await page.evaluate("""() => {
            const websiteLink = Array.from(document.querySelectorAll('a')).find(a => 
                a.textContent.toLowerCase().includes('website') || 
                a.classList.contains('website') ||
                a.classList.contains('business-website') ||
                a.href.includes('?y_source=') ||
                a.href.match(/https?:\\/\\/(?!www\\.yellowpages\\.com)/)
            );
            
            if (websiteLink) {
                // Extract the actual URL from YP redirect links
                const url = websiteLink.href;
                if (url.includes('?y_source=')) {
                    const decoded = decodeURIComponent(url.split('?y_source=')[0]);
                    return decoded;
                }
                return url;
            }
            
            // Look for schema markup
            const websiteSchema = document.querySelector('[itemprop="url"]');
            if (websiteSchema) {
                return websiteSchema.getAttribute('content') || websiteSchema.textContent.trim();
            }
            
            return '';
        }""")
        
        if website and not website.startswith('https://www.yellowpages.com'):
            result['website'] = website
            
        # Get hours of operation
        hours = await page.evaluate("""() => {
            // Try to find hours of operation from various sources
            const hoursContainer = 
                document.querySelector('.hours-wrapper') ||
                document.querySelector('.hours') ||
                document.querySelector('.business-hours') ||
                document.querySelector('[itemprop="openingHours"]')?.parentElement;
                
            if (hoursContainer) {
                const hoursText = hoursContainer.textContent.trim();
                return hoursText;
            }
            
            // Check for hours in schema markup
            const hoursMeta = Array.from(document.querySelectorAll('[itemprop="openingHours"]'));
            if (hoursMeta.length) {
                return hoursMeta.map(el => el.getAttribute('content') || el.textContent.trim()).join('; ');
            }
            
            return '';
        }""")
        
        if hours:
            result['hours'] = hours
        
        # Get categories/cuisine type
        categories = await page.evaluate("""() => {
            // Look for categories in various places
            const categoryLinks = Array.from(document.querySelectorAll('.categories a, .business-categories a'));
            if (categoryLinks.length) {
                return categoryLinks.map(a => a.textContent.trim()).join(', ');
            }
            
            // Try other sources
            const categoryContainer = 
                document.querySelector('.categories') ||
                document.querySelector('.business-categories');
                
            if (categoryContainer) {
                return categoryContainer.textContent.trim();
            }
            
            return '';
        }""")
        
        if categories:
            result['categories'] = categories
        
        return result

    def save_results(self):
        """Save results in both JSON and CSV formats with duplicate columns handled"""
        if not self.results:
            logger.error("No results to save - scraping failed")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = self.output_dir / f'yellowpages_{timestamp}.json'
        csv_path = self.output_dir / f'yellowpages_{timestamp}.csv'
        
        # Save JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        # Prepare CSV data - normalize fields
        csv_data = []
        
        # Get all unique field names
        all_fields = set()
        for item in self.results:
            all_fields.update(item.keys())
        
        # Define a fixed set of columns for consistent output
        primary_fields = [
            'name', 'phone', 'email', 'website',
            'street_address', 'city', 'state', 'zip_code', 'country',
            'latitude', 'longitude',
            'categories', 'cuisine', 
            'price_range', 'rating', 'review_count',
            'hours', 'payment_methods',
            'neighborhoods',
            'listing_url', 'scraped_at'
        ]
        
        # Include any other fields found in the data
        ordered_fields = [f for f in primary_fields if f in all_fields]
        extra_fields = sorted(f for f in all_fields if f not in primary_fields)
        ordered_fields.extend(extra_fields)
        
        # Prepare the CSV rows with consistent columns
        for item in self.results:
            row = {field: item.get(field, '') for field in ordered_fields}
            csv_data.append(row)
        
        # Save CSV
        if csv_data:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_fields)
                writer.writeheader()
                writer.writerows(csv_data)
        
        logger.info(f"Successfully saved {len(self.results)} results")
        logger.info(f"JSON: {json_path}")
        logger.info(f"CSV: {csv_path}")

    # Add methods to rotate fingerprints and cookies
    def generate_fingerprint(self):
        """Generate a random but plausible browser fingerprint"""
        return {
            "screen": {
                "width": random.choice([1366, 1440, 1536, 1600, 1920, 2560]),
                "height": random.choice([768, 800, 864, 900, 1080, 1440]),
                "colorDepth": random.choice([24, 30, 32]),
            },
            "userAgent": random.choice(self.user_agents),
            "timezone": random.choice([-7, -6, -5, -4]),
            "language": random.choice(["en-US", "en-GB", "en-CA"]),
            "sessionStorage": random.choice([True, False]),
            "localStorage": True,
            "indexedDb": random.choice([True, False]),
            "cpuClass": random.choice(["unknown", None]),
            "platform": random.choice(["Win32", "MacIntel", "Linux x86_64"]),
            "doNotTrack": random.choice([None, "1", "0"]),
            "plugins": random.randint(3, 10),
            "canvas": ''.join(random.choices(string.ascii_letters + string.digits, k=20)),
            "webgl": ''.join(random.choices(string.ascii_letters + string.digits, k=20)),
            "webglVendor": random.choice([
                "Google Inc. (NVIDIA)", 
                "Google Inc. (Intel)", 
                "Google Inc. (AMD)",
                "Apple Computer, Inc."
            ]),
            "adBlock": random.choice([True, False]),
            "touchSupport": random.choice([True, False])
        }

    async def handle_cookies(self, page: Page):
        """Handle cookie consent dialogs"""
        cookie_selectors = [
            "button[aria-label*='Accept']",
            "button[title*='Accept']",
            "button[data-testid*='cookie-policy']",
            "button:has-text('Accept')",
            "button:has-text('Accept All')",
            "button:has-text('I Accept')",
            "button:has-text('Allow')",
            "button:has-text('Allow All')",
            "button:has-text('Agree')",
            "button:has-text('OK')",
            "button:has-text('Got it')",
            "[id*='cookie'] button",
            "[class*='cookie'] button",
            ".consent button",
            "#consent button",
            ".gdpr button",
            "#gdpr button"
        ]
        
        for selector in cookie_selectors:
            try:
                if await page.query_selector(selector):
                    logger.info(f"Handling cookie consent with selector: {selector}")
                    await page.click(selector)
                    await asyncio.sleep(0.5 + random.random() * 0.5)
                    return True
            except Exception as e:
                continue
                
        return False


async def main():
    """Run the scraper with command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape YellowPages listings with enhanced deduplication')
    parser.add_argument('--search', type=str, default='restaurants', help='Search term (e.g., restaurants, plumbers)')
    parser.add_argument('--location', type=str, default='los-angeles-ca', help='Location (e.g., los-angeles-ca, new-york-ny)')
    parser.add_argument('--pages', type=int, default=3, help='Number of pages to scrape')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    
    args = parser.parse_args()
    
    scraper = EnhancedYellowPagesScraper(
        search_term=args.search,
        location=args.location,
        page_limit=args.pages
    )
    
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())