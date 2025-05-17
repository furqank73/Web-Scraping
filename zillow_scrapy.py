import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random

# Cookies and headers from your browser session
cookies = {
    'zguid': '24|%244001b58f-23cd-419b-9ca5-422e00b76ae0',
    'zgsession': '1|9e85e314-4935-4a99-b52e-32c731e787eb',
    '_ga': 'GA1.2.1561133998.1745841052',
    'zjs_anonymous_id': '%224001b58f-23cd-419b-9ca5-422e00b76ae0%22',
    'zjs_user_id': 'null',
    'zg_anonymous_id': '%22f713a065-0586-416f-9019-75cd609c5076%22',
    'pxcts': '09b33f38-2427-11f0-b7d6-3838b359465e',
    '_pxvid': '09b333cd-2427-11f0-b7d6-540e13a31bf1',
    '_gcl_au': '1.1.116495423.1745841057',
    '_tt_enable_cookie': '1',
    '_ttp': '01JSY3RVTH8G641KRG1C2W95YB_.tt.1',
    '_scid': '2E_FgT4JliKcm48a47vsoeBUgQ-3c0gf',
    '_fbp': 'fb.1.1745841057991.297060372190490785',
    '_pin_unauth': 'dWlkPU1HSmxZbVkwWlRFdFlUSXhaaTAwTlRnMkxUa3lPV1l0TVRKbE1tVmhZV1UwWW1FMA',
    'DoubleClickSession': 'true',
    '_gac_UA-21174015-56': '1.1746206602.Cj0KCQjw2tHABhCiARIsANZzDWpYd0uSw3Q2voTFnW8dIKndakHAe3j6J6JnwQkjCEQh6s_6u4Kp16kaAv1fEALw_wcB',
    '_gcl_gs': '2.1.k1$i1746206595$u52198818',
    '_gcl_aw': 'GCL.1746528496.Cj0KCQjw2tHABhCiARIsANZzDWpYd0uSw3Q2voTFnW8dIKndakHAe3j6J6JnwQkjCEQh6s_6u4Kp16kaAv1fEALw_wcB',
    '_gid': 'GA1.2.54033918.1747410485',
    '_ScCbts': '%5B%22565%3Bchrome.2%3A2%3A5%22%5D',
    '_sctr': '1%7C1747335600000',
    '_clck': 'bua8wx%7C2%7Cfvz%7C0%7C1944',
    'JSESSIONID': '28C56A32552C8DFEC39CD02F688EE904',
    'AWSALB': 'JeLeJUgNcNhh04am3I6Z/Kl9T2ju+4f1LpBFEBJTyvtLDKfCZA1rz1oQC3P2zJER8i8nd/XWSGrWyzKDXFZGLHdpme3BfVGt/hIQTn7HR16QEjxbjFpVi4KSzGSm',
    'AWSALBCORS': 'JeLeJUgNcNhh04am3I6Z/Kl9T2ju+4f1LpBFEBJTyvtLDKfCZA1rz1oQC3P2zJER8i8nd/XWSGrWyzKDXFZGLHdpme3BfVGt/hIQTn7HR16QEjxbjFpVi4KSzGSm',
    'web-platform-data': '%7B%22wp-dd-rum-session%22%3A%7B%22doNotTrack%22%3Atrue%7D%7D',
    '_px3': 'aaf553d602ef8f7e59496ab095cb48be49babb1ab87444c798f6fe3a5a7b2749:D7hN6vKe6K0ED26yb1cBYbnv+ZUP8k109EJEeQRfUX1Q0ylsULT1jLGeCrjHhePlXHCEIbsN/YwzDm1miZdydA==:1000:qioe1Ivh5YlwyCK+9A6UnMnjV26QnkIDItvLfvDSFby8A0wzgv4UX6cijGxf2YggSCfkcioKZ718HOGNWbtesFMt99Ap6ItspRvFwXTXWHn9P3K+tcHGAAs38Eb99Dn1XQWfZQCsJID3ZhqTEzJXhoHdK5W3B/953O1SWLC/A0lkKbDBLcp26VpKll8o/9kPFYoK2bg+tqGnqPoS2sqp5Iek47N1d5uQ+RbVPEqHuLI=',
    '_scid_r': '_s_FgT4JliKcm48a47vsoeBUgQ-3c0gfrqP5_A',
    '_rdt_uuid': '1745841057647.cd28d378-e670-4709-92b5-372906e8bf0f',
    'tfpsi': '68744c2d-8459-4aae-95ac-31537f6809e3',
    '_derived_epik': 'dj0yJnU9eGJnWXE2RUltenlxYUNpbDk0d2tobEphYmlIVGYxVEYmbj1qZjNZSDhXa0VWZS1HdG9qbU1nV3lnJm09MSZ0PUFBQUFBR2dvbjlFJnJtPTEmcnQ9QUFBQUFHZ29uOUUmc3A9Mg',
    '_uetsid': '392f0380326d11f08bf469540ceb93d9',
    '_uetvid': '0c4757f0242711f0adaaf534e617191d',
    'ttcsid_CN5P33RC77UF9CBTPH9G': '1747492814806::ljspVB9Z3vc106kgEMjw.25.1747492920535',
    'ttcsid': '1747492814807::A5CN5r0ppzyXcOC4f5k1.25.1747492920536',
    'search': '6|1750084923770%7Crect%3D41.13770444804806%2C-73.25205845413966%2C40.254790439080075%2C-74.70774693070216%26rid%3D6181%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26listPriceActive%3D1%26fs%3D1%26fr%3D0%26mmm%3D0%26rs%3D0%26singlestory%3D0%26housing-connector%3D0%26parking-spots%3Dnull-%26abo%3D0%26garage%3D0%26pool%3D0%26ac%3D0%26waterfront%3D0%26finished%3D0%26unfinished%3D0%26cityview%3D0%26mountainview%3D0%26parkview%3D0%26waterview%3D0%26hoadata%3D1%26zillow-owned%3D0%263dhome%3D0%26showcase%3D0%26featuredMultiFamilyBuilding%3D0%26onlyRentalStudentHousingType%3D0%26onlyRentalIncomeRestrictedHousingType%3D0%26onlyRentalMilitaryHousingType%3D0%26onlyRentalDisabledHousingType%3D0%26onlyRentalSeniorHousingType%3D0%26commuteMode%3Ddriving%26commuteTimeOfDay%3Dnow%09%096181%09%7B%22isList%22%3Atrue%2C%22isMap%22%3Afalse%7D%09%09%09%09%09',
    '_clsk': 'homc49%7C1747492983336%7C2%7C0%7Cn.clarity.ms%2Fcollect',
    '__gads': 'ID=372f910b152e5baf:T=1745841126:RT=1747493034:S=ALNI_MYXPQXBCqOgRCCYeNYWk0ySmCdi0g',
    '__gpi': 'UID=0000109783baafea:T=1745841126:RT=1747493034:S=ALNI_MZqiuxxtpF6CDq_3aiKumVzcdnkhg',
    '__eoi': 'ID=8dc89d0f997d1744:T=1745841126:RT=1747493034:S=AA-AfjY5PrC70-03OjnCT2bllAX1',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
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

params = {
    'searchQueryState': '{"pagination":{},"isMapVisible":false,"mapBounds":{"west":-74.70774693070216,"east":-73.25205845413966,"south":40.254790439080075,"north":41.13770444804806},"usersSearchTerm":"New York, NY","regionSelection":[{"regionId":6181,"regionType":6}],"filterState":{"sort":{"value":"globalrelevanceex"}},"isListVisible":true,"mapZoom":9}',
}

def scrape_zillow_listings(location, max_pages=1):
    """
    Scrape real estate listings from zillow.com
    
    Args:
        location (str): Location to search for (e.g., 'New-York-NY')
        max_pages (int): Maximum number of pages to scrape
    
    Returns:
        list: List of property dictionaries
    """
    all_properties = []
    base_url = f"https://www.zillow.com/{location}/"
    
    for page in range(1, max_pages + 1):
        # Update pagination in params
        params['searchQueryState'] = json.loads(params['searchQueryState'])
        params['searchQueryState']['pagination'] = {"currentPage": page}
        params['searchQueryState'] = json.dumps(params['searchQueryState'])
        
        try:
            print(f"Scraping page {page} for {location}...")
            
            # Add a random delay between requests to avoid rate limiting
            if page > 1:
                delay = random.uniform(3, 7)
                print(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
            
            # Make the request
            response = requests.get(base_url, params=params, cookies=cookies, headers=headers)
            
            # Check if request was successful
            if response.status_code != 200:
                print(f"Failed to retrieve page {page}. Status code: {response.status_code}")
                break
                
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try to find the property data in the NEXT_DATA script
            next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
            
            if next_data_script:
                try:
                    data = json.loads(next_data_script.string)
                    homes = data['props']['pageProps']['searchPageState']['cat1']['searchResults']['listResults']
                    
                    for home in homes:
                        # Extract home info data
                        home_info = home.get('hdpData', {}).get('homeInfo', {})
                        
                        home_data = {
                            # Basic info
                            "zpid": home.get('zpid', None),
                            "home_type": home_info.get('homeType', None),
                            "posted": str(home_info.get('daysOnZillow', None)) + ' days ago',
                            "time_on_zillow": home_info.get('timeOnZillow', None),
                            "home_URL": home.get('detailUrl', None),
                            "home_main_image": home.get('imgSrc', None),
                            "home_status": home.get('statusType', None),
                            "home_status_detailed": home.get('sgapt', None),
                            "raw_home_status": home.get('rawHomeStatusCd', None),
                            "marketing_status": home.get('marketingStatusSimplifiedCd', None),
                            
                            # Pricing & Value info
                            "home_price": home.get('price', None),
                            "unformatted_price": home.get('unformattedPrice', None),
                            "country_currency": home.get('countryCurrency', None),
                            "zestimate": home.get('zestimate', None),
                            "rent_zestimate": home_info.get('rentZestimate', None),
                            "tax_assessed_value": home_info.get('taxAssessedValue', None),
                            
                            # Location info
                            "home_address": home.get('address', None),
                            "address_street": home.get('addressStreet', None),
                            "address_city": home.get('addressCity', None),
                            "address_state": home.get('addressState', None),
                            "address_zipcode": home.get('addressZipcode', None),
                            "unit": home_info.get('unit', None),
                            "latitude": home.get('latLong', {}).get('latitude', None),
                            "longitude": home.get('latLong', {}).get('longitude', None),
                            
                            # Property details
                            "num_beds": home.get('beds', None),
                            "num_baths": home.get('baths', None),
                            "home_area": home.get('area', None),
                            "living_area": home_info.get('livingArea', None),
                            "flex_field_text": home.get('flexFieldText', None),
                            
                            # Additional features
                            "broker_name": home.get('brokerName', None),
                            "has_3d_model": home.get('has3DModel', False),
                            "has_video": home.get('hasVideo', False),
                            "is_featured": home.get('isFeaturedListing', False),
                            "is_showcase_listing": home.get('isShowcaseListing', False),
                            "is_zillow_owned": home.get('isZillowOwned', False),
                            "is_undisclosed_address": home.get('isUndisclosedAddress', False),
                            "is_user_claiming_owner": home.get('isUserClaimingOwner', False),
                            "is_user_confirmed_claim": home.get('isUserConfirmedClaim', False),
                            "should_highlight": home_info.get('shouldHighlight', False),
                            "is_featured_home_builder": home_info.get('isPremierBuilder', False),
                            "is_non_owner_occupied": home_info.get('isNonOwnerOccupied', None),
                            "listing_sub_type_is_FSBA": home_info.get('listing_sub_type', {}).get('is_FSBA', None)
                        }
                        
                        # Add all photo URLs if available
                        if 'carouselPhotos' in home and home['carouselPhotos']:
                            photo_urls = [photo.get('url') for photo in home['carouselPhotos']]
                            home_data["all_photos"] = photo_urls
                            home_data["photo_count"] = len(photo_urls)
                        
                        all_properties.append(home_data)
                    
                    print(f"Found {len(homes)} properties on page {page}")
                    
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error parsing JSON data: {e}")
            else:
                print("Could not find NEXT_DATA script in page")
                
            # Save the HTML (for debugging)
            with open(f"zillow_{location}_page_{page}.html", "w", encoding="utf-8") as f:
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
    # Location to search - format as "City-State" (e.g., "New-York-NY")
    location = "New-York-NY"
    
    # Number of pages to scrape
    max_pages = 20
    
    # Scrape the properties
    properties = scrape_zillow_listings(location, max_pages)
    
    # Save to JSON
    save_to_json(properties, f"zillow_{location}_listings.json")
    
    # Save to CSV
    save_to_csv(properties, f"zillow_{location}_listings.csv")
    
    print(f"Scraped a total of {len(properties)} properties")

if __name__ == "__main__":
    main()