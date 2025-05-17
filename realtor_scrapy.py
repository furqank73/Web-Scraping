import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random

# Cookies and headers from your browser session
cookies = {
    '_cq_duid': '1.1747377325.eXFotxKrthzMsk4I',
    '_cq_suid': '1.1747377325.vebUfXMfypx8awDi',
    'split': 'n',
    '__vst': '53dfc4f0-7854-47c3-b7af-9e8875f1b5d6',
    '__ssn': '0f66c77c-9f13-46d3-824e-8f4f2ffe5f3f',
    '__ssnstarttime': '1747377330',
    '__bot': 'false',
    'isAuth0GnavEnabled': 'C',
    'permutive-id': '969d3150-a6d3-4f8b-9f52-cbd41706d7cc',
    '_lr_env_src_ats': 'false',
    '_pbjs_userid_consent_data': '3524755945110770',
    '__split': '90',
    'G_ENABLED_IDPS': 'google',
    'pbjs-unifiedid': '%7B%22TDID%22%3A%22af5277fc-eb99-4947-8c1b-15c0c04d2e1b%22%2C%22TDID_LOOKUP%22%3A%22TRUE%22%2C%22TDID_CREATED_AT%22%3A%222025-04-16T06%3A35%3A36%22%7D',
    'pbjs-unifiedid_cst': 'VyxHLMwsHQ%3D%3D',
    'AMCVS_8853394255142B6A0A4C98A4%40AdobeOrg': '1',
    '_gcl_au': '1.1.1406284740.1747377335',
    'claritas_24hrexp_sitevisit': 'true',
    's_ecid': 'MCMID%7C10597946375941487181428432323772462246',
    '_ga': 'GA1.1.772119589.1747377337',
    'ajs_anonymous_id': '98ed4ef5-f8fd-4f1a-8c74-7af4b3648d6e',
    '__spdt': 'e28020c1020b43e090bd33f3b4ff9cf9',
    '_lr_sampling_rate': '0',
    'crto_is_user_optout': 'false',
    'crto_mapped_user_id_NewsAndInsights': 'In-sul8ySnVMeVdZbDdjWEklMkJyaWJ0MTJDTFpqQjJ4SHR3dnR1RE1qeUxMVzhjY2MlM0Q',
    'crto_mapped_user_id_ForSale': '1QSthl8ySnVMeVdZbDdjWEklMkJyaWJ0MTJDTFJ6V3RrbiUyQmh1OWclMkJGeWZTNHhZMjM0JTNE',
    'crto_mapped_user_id_Rental': 'zFJOHl8ySnVMeVdZbDdjWEklMkJyaWJ0MTJDTGRXRDFQN2dXUWV4dDJDQ0FFJTJCc3dLVSUzRA',
    'panoramaId_expiry': '1747463742427',
    '_cc_id': '65b1c99e0f8550094187450d35526aae',
    'panoramaId': 'd0f54cc7db747c2b6ad05ebb4e08a9fb927a0d8902ac3b949b51c8ae71f9edee',
    'claritas_24hrexp_search': 'true',
    '__gsas': 'ID=7d70597543493d19:T=1747377390:RT=1747377390:S=ALNI_MbfyEMou4OU60t44NCNq9T9FDTx4Q',
    'kampyle_userid': 'ed8c-2cae-9bd4-dee8-c8a7-0812-cf46-ef28',
    'claritas_24hrexp_ldp': 'true',
    'ldp-environmental-risk': 'true',
    'ldp-neighborhood': 'true',
    'split_tcv': '174',
    '_lr_retry_request': 'true',
    'AMCV_8853394255142B6A0A4C98A4%40AdobeOrg': '-1124106680%7CMCIDTS%7C20225%7CMCMID%7C10597946375941487181428432323772462246%7CMCAAMLH-1748058824%7C3%7CMCAAMB-1748058824%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1747461224s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0',
    'ab.storage.deviceId.7cc9d032-9d6d-44cf-a8f5-d276489af322': 'g%3Af51a476e-407f-af9b-3fc6-6515775e2411%7Ce%3Aundefined%7Cc%3A1747377333670%7Cl%3A1747454026544',
    'ab.storage.userId.7cc9d032-9d6d-44cf-a8f5-d276489af322': 'g%3Avisitor_53dfc4f0-7854-47c3-b7af-9e8875f1b5d6%7Ce%3Aundefined%7Cc%3A1747377333665%7Cl%3A1747454026544',
    '_parsely_session': '{%22sid%22:7%2C%22surl%22:%22https://www.realtor.com/realestateandhomes-search/New-York%22%2C%22sref%22:%22https://www.realtor.com/%22%2C%22sts%22:1747454033095%2C%22slts%22:1747423332039}',
    '_parsely_visitor': '{%22id%22:%22pid=cd8dffdb-66ff-4b36-affa-c52df4031905%22%2C%22session_count%22:7%2C%22last_session_ts%22:1747454033095}',
    'AMP_MKTG_c07a79acf5': 'JTdCJTIycmVmZXJyZXIlMjIlM0ElMjJodHRwcyUzQSUyRiUyRnd3dy5nb29nbGUuY29tJTJGJTIyJTJDJTIycmVmZXJyaW5nX2RvbWFpbiUyMiUzQSUyMnd3dy5nb29nbGUuY29tJTIyJTdE',
    'kampyleUserSession': '1747455183939',
    'kampyleUserSessionsCount': '2',
    'kampyleUserPercentile': '61.26149430710905',
    'srchID': 'e35ea78814d6476a8e8ffa18f8ac70db',
    'criteria': 'sprefix%3D%252Fnewhomecommunities%26area_type%3Dstate%26pg%3D1%26state_code%3DNY%26state%3DNew%2520York%26state_id%3DNY%26loc%3DNew%2520York%26locSlug%3DNew-York',
    '__gads': 'ID=be6f491171339365:T=1747377335:RT=1747457033:S=ALNI_MZIpnR0ZBVTfUeRtUTNBZd37qRJTQ',
    '__gpi': 'UID=000010b04809c144:T=1747377335:RT=1747457033:S=ALNI_MZRDB-cBHF75qTjQlG7Q9ps4VuebA',
    '__eoi': 'ID=e1bbef026a616dc2:T=1747377335:RT=1747457033:S=AA-AfjYQd8-H_ydLxh2hX-LtKxnx',
    'KP_UIDz-ssn': '02DE4MyoGfusCzwSSvYttSYeAyx5OLQtzeaeJchzDIy4KIoCo75HMKgdFUtY2rvBnRaSD9pQ4mg3UStSM4ipXNlG7zORpJM7RGCAlWdJ5MxKrjF5S5RSq8K3Ulo6oSwCh2O8GCUMZHeJXW8WyC3cVI3ZYnIatjZsQQRJ6Gc325',
    'KP_UIDz': '02DE4MyoGfusCzwSSvYttSYeAyx5OLQtzeaeJchzDIy4KIoCo75HMKgdFUtY2rvBnRaSD9pQ4mg3UStSM4ipXNlG7zORpJM7RGCAlWdJ5MxKrjF5S5RSq8K3Ulo6oSwCh2O8GCUMZHeJXW8WyC3cVI3ZYnIatjZsQQRJ6Gc325',
    'cto_bundle': 'gC5DQV9INllsUzByJTJCOGQ3dVJBa3ZVekF3MkE4R2lPVjd4MXBqJTJGRnVMNXF4T2xYZiUyQlFVRkhoU3NrRHdYcGVCSkJRWFB5JTJGUENkOWxESFpwRzBZMjB5UUpYd1NWZkY0NWFHbThqcjhWYzFkY1Z0RG1pT2tnQnl0TFkxbjVHNHBWSnBXcGFGOVA1Tnp4OHNBcmtnaFVLJTJGOUUlMkJnUmZVY0IlMkZnNzBhemhaNUFRT25ZRE1Ya3pWV0ZReExDNEdiNlRLSkVFUElZNHZkQUxSRks4WXBHVjBhcG9QemRqdWclM0QlM0Q',
    'leadid_token-27789EFE-7A9A-DB70-BB9B-97D9B7057DBB-01836014-7527-FD48-9B7F-1A40A9705CFE': '5C67FF47-D0C0-40B1-463B-ECFBB1D06C73',
    '_ga_07XBH6XBNS': 'GS2.1.s1747454027$o5$g1$t1747457150$j0$l0$h1652739724',
    '_ga_MS5EHT6J6V': 'GS2.1.s1747454028$o5$g1$t1747457151$j0$l0$h0',
    '_uetsid': 'fa49a470321f11f0bee381a7ad9e56c4|jjrxi|2|fvz|0|1962',
    'AMP_c07a79acf5': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJiNzA5NGIzMS0yODVjLTQxOTgtOGU5ZC1mMTg0MDg0ZGRlMTklMjIlMkMlMjJ1c2VySWQlMjIlM0ElMjI1M2RmYzRmMC03ODU0LTQ3YzMtYjdhZi05ZTg4NzVmMWI1ZDYlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzQ3NDU0MDI5NjczJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc0NzQ1NzE1NDQ4NyUyQyUyMmxhc3RFdmVudElkJTIyJTNBNDMlMkMlMjJwYWdlQ291bnRlciUyMiUzQTclN0Q=',
    '_uetvid': 'fa49b2d0321f11f0a1a9b78e58153a34|lhrzwc|1747457159912|7|1|bat.bing.com/p/insights/c/j',
    'ab.storage.sessionId.7cc9d032-9d6d-44cf-a8f5-d276489af322': 'g%3A4f6db884-155f-3074-3045-46d46da7c491%7Ce%3A1747458962503%7Cc%3A1747454026542%7Cl%3A1747457162503',
    'kampyleSessionPageCounter': '4',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'referer': 'https://www.realtor.com/realestateforsale',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
}

def scrape_realtor_listings(location, max_pages=1):
    """
    Scrape real estate listings from realtor.com
    
    Args:
        location (str): Location to search for (e.g., 'New-York')
        max_pages (int): Maximum number of pages to scrape
    
    Returns:
        list: List of property dictionaries
    """
    all_properties = []
    
    for page in range(1, max_pages + 1):
        # Construct URL with pagination
        url = f'https://www.realtor.com/realestateandhomes-search/{location}/pg-{page}'
        
        try:
            print(f"Scraping page {page} for {location}...")
            
            # Add a random delay between requests to avoid rate limiting
            if page > 1:
                delay = random.uniform(3, 7)
                print(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
            
            # Make the request
            response = requests.get(url, cookies=cookies, headers=headers)
            
            # Check if request was successful
            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                break
                
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try to find the property data in the page
            # Realtor.com often embeds data in JSON scripts
            script_tags = soup.find_all('script', {'type': 'application/json'})
            
            # Extract property data
            properties = []
            for script in script_tags:
                if script.string and '"props":' in script.string:
                    try:
                        json_data = json.loads(script.string)
                        if 'props' in json_data and 'pageProps' in json_data['props']:
                            page_props = json_data['props']['pageProps']
                            if 'properties' in page_props:
                                properties = page_props['properties']
                                break
                    except json.JSONDecodeError:
                        continue
            
            if not properties:
                # Alternative method: try to find property cards directly in HTML
                property_cards = soup.select('div[data-testid="property-card"]')
                
                for card in property_cards:
                    try:
                        # Extract basic property information
                        price_elem = card.select_one('span[data-testid="property-price"]')
                        address_elem = card.select_one('div[data-testid="property-address"]')
                        beds_elem = card.select_one('li[data-testid="property-meta-beds"] span')
                        baths_elem = card.select_one('li[data-testid="property-meta-baths"] span')
                        sqft_elem = card.select_one('li[data-testid="property-meta-sqft"] span')
                        
                        # Create property dictionary
                        property_data = {
                            'price': price_elem.text if price_elem else 'N/A',
                            'address': address_elem.text if address_elem else 'N/A',
                            'beds': beds_elem.text if beds_elem else 'N/A',
                            'baths': baths_elem.text if baths_elem else 'N/A',
                            'sqft': sqft_elem.text if sqft_elem else 'N/A',
                            'url': card.select_one('a')['href'] if card.select_one('a') else None
                        }
                        
                        properties.append(property_data)
                    except Exception as e:
                        print(f"Error parsing property card: {e}")
            
            # Add properties to overall list
            all_properties.extend(properties)
            print(f"Found {len(properties)} properties on page {page}")
            
            # Save the HTML (for debugging)
            with open(f"realtor_{location}_page_{page}.html", "w", encoding="utf-8") as f:
                f.write(response.text)
                
        except Exception as e:
            print(f"Error scraping page {page}: {e}")
    
    return all_properties

def save_to_csv(properties, filename):
    """
    Save property data to a CSV file
    
    Args:
        properties (list): List of property dictionaries
        filename (str): Output filename
    """
    if not properties:
        print("No properties to save.")
        return
    
    # Get all unique keys from all properties
    fieldnames = set()
    for prop in properties:
        fieldnames.update(prop.keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(properties)
    
    print(f"Saved {len(properties)} properties to {filename}")

def save_to_json(properties, filename):
    """
    Save property data to a JSON file

    Args:
        properties (list): List of property dictionaries
        filename (str): Output filename
    """
    if not properties:
        print("No properties to save.")
        return

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(properties, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(properties)} properties to {filename}")

def main():
    # Location to search - replace spaces with hyphens
    location = "New-York"
    
    # Number of pages to scrape
    max_pages = 2
    
    # Scrape the properties
    properties = scrape_realtor_listings(location, max_pages)
    
    # Save to CSV
    save_to_json(properties, f"realtor_{location}_listings.json")
    
    print(f"Scraped a total of {len(properties)} properties")

if __name__ == "__main__":
    main()