import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import http.client
import json
from firecrawl import FirecrawlApp
import openai
import pandas as pd
import logging
from pydantic import BaseModel, Field
from openai import OpenAI

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_API_HOST = os.getenv('SERPER_API_HOST')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
WEBPAGE_URL = os.getenv('WEBPAGE_URL')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

firecrawl_app = FirecrawlApp(api_key=os.getenv('FIRECRAWL_API_KEY'))
openai.api_key = OPENAI_API_KEY
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def search_google_serper(query):
    """Perform a Google search using the Serper.dev API."""
    try:
        conn = http.client.HTTPSConnection(SERPER_API_HOST)
        payload = json.dumps({
            "q": query,
            "gl": "de"
        })
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        conn.request("POST", "/search", payload, headers)
        res = conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        logging.error(f"Error during Google search with Serper.dev: {e}")
        raise

def clean_url_with_openai(url):
    """Use OpenAI to extract the base URL from a given link."""
    prompt = f"Extract the base URL from the following link: {url}. Return only the base URL without any query parameters or fragments. If the URL already ONLY contains the base URL, return it as it is."

    class CleanLink(BaseModel):
        clean_link: str = Field(description="An https link which only contains the base url. For example 'https://www.otinga.de' or 'https://www.google.de'.")

    try:
        response = openai_client.beta.chat.completions.parse(
                model="gpt-4o-2024-08-06",
                temperature=0.0,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": url},
                ],
                response_format=CleanLink,
            )
        clean_link_repsonse = response.choices[0].message.parsed
        print("clean_link_repsonse", clean_link_repsonse)
        cleaned_url = clean_link_repsonse.clean_link
        return cleaned_url if cleaned_url else None

    except Exception as e:
        print(f"Error during OpenAI API call: {e}")
        return None

def crawl_url_with_firecrawl(url):
    """Crawl a URL using the Firecrawl API."""
    try:
        map_result = firecrawl_app.map_url(url, params={
            'includeSubdomains': True
        })
        return map_result
    except Exception as e:
        logging.error(f"Error during crawling URL with Firecrawl: {e}")
        raise

def scrape_impressum_url(url):
    """Scrape the Impressum page if found."""
    try:
        response = firecrawl_app.scrape_url(url=url, params={
            'formats': ['markdown']
        })
        return response
    except Exception as e:
        logging.error(f"Error during scraping Impressum URL: {e}")
        raise

def find_legal_info_link(links):
    """Check for potential legal information links using OpenAI."""
    print("find_legal_info_link links", links)

    class ImpressumLink(BaseModel):
        legal_information_link: str = Field(description="An https link which could provide legal information about the company. For example 'https://www.otinga.de/policies/legal-notice'. Here the 'legal-notice' identifies the link as the link which provides legal information")

    prompt = "Identify if any of the following links potentially provide legal information about a company. Return the link if found, otherwise return None."
    try:
        # Convert links to a list of message objects
        link_messages = [{"role": "user", "content": link} for link in links]

        response = openai_client.beta.chat.completions.parse(
                model="gpt-4o-2024-08-06",
                temperature=0.0,
                messages=[
                    {"role": "system", "content": prompt},
                    *link_messages,  # Unpack the list of link messages
                ],
                response_format=ImpressumLink,
            )
        print("response find_legal_info_link", response)
        potential_link = response.choices[0].message.parsed
        print("potential_link", potential_link)
        legal_information_link = potential_link.legal_information_link
        return legal_information_link if legal_information_link else None
    
    except Exception as e:
        logging.error(f"Error during OpenAI API call for legal info link: {e}")
        return None

def extract_company_info(content):
    """Call OpenAI API for extracting company name and register number."""

    class CompanyInformation(BaseModel):
        company_name: str = Field(description="The complete name of the company. For example 'otinga GmbH' or 'Trustyourvc GmbH'")
        register_number: str = Field(description="The register number of the company. For example 'HRB 24991' or 'HRB 261637 B")
        
    try:
        prompt = f"Extract the company_name and register number from the extracted html content of the Impressum page"

        response = openai_client.beta.chat.completions.parse(
                model="gpt-4o-2024-08-06",
                temperature=0.0,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": content},
                ],
                response_format=CompanyInformation,
            )
        company_information = response.choices[0].message.parsed
        print("company_information", company_information)

        logging.info(f"Extracted company name: {company_information.company_name}")
        logging.info(f"Extracted register number: {company_information.register_number}")
        company_name = company_information.company_name
        register_number = company_information.register_number
        return company_name, register_number
    
    except json.JSONDecodeError:
        logging.error("JSON decode error while extracting company info.")
        return None, None
    except Exception as e:
        logging.error(f"Error during OpenAI API call: {e}")
        return None, None

def write_to_excel(data, file_name="company_data.xlsx"):
    """Write data to an XLSX file."""
    try:
        df = pd.DataFrame(data, columns=["Title", "Company Name", "Register Number"])
        df.to_excel(file_name, index=False)
        print(f"Data written to {file_name}")
    except Exception as e:
        logging.error(f"Error writing data to Excel: {e}")
        raise

# Step 1: Fetch the webpage content
try:
    url = WEBPAGE_URL
    response = requests.get(url)
    html_content = response.content
except Exception as e:
    logging.error(f"Error fetching webpage content: {e}")
    raise

# Step 2: Parse the HTML content
try:
    soup = BeautifulSoup(html_content, 'html.parser')
except Exception as e:
    logging.error(f"Error parsing HTML content: {e}")
    raise

# Step 3: Find the starting point using the unique class "article articlel filter clearfix"
try:
    filter_section = soup.find('article', class_='article articlel filter clearfix')
except Exception as e:
    logging.error(f"Error finding filter section: {e}")
    raise

# Step 4: Find the <div class="list"> after the filter section
try:
    list_section = filter_section.find_next('div', class_='list')
except Exception as e:
    logging.error(f"Error finding list section: {e}")
    raise

# List to store Excel data
excel_data = []

# Step 5: Loop through the articles inside the list and get the titles
try:
    for article in list_section.find_all('article'):
        title = article.find('a', class_='name').get('title')
        print(f"Searching for: {title}")

        # Step 6: Use the title as the search query in Serper.dev
        search_results = search_google_serper(title)

        # Step 7: Display the first organic result (position 1)
        if search_results and 'organic' in search_results:
            first_result = search_results['organic'][0]
            first_link = first_result['link']
            print(f"First result link for {title}: {first_link}")

            cleaned_first_link = clean_url_with_openai(first_link)

            # Step 8: Crawl the first result link with Firecrawl
            firecrawl_result = crawl_url_with_firecrawl(cleaned_first_link)
            if firecrawl_result and firecrawl_result.get("success"):
                impressum_link = None
                for link in firecrawl_result["links"]:
                    if "impressum" in link.lower():
                        impressum_link = link
                        print(f"Found Impressum: {impressum_link}")
                        break

                # If no "impressum" link is found, use OpenAI to find a potential legal info link
                # if not impressum_link:
                #     impressum_link = find_legal_info_link(firecrawl_result["links"])
                #     if impressum_link:
                #         print(f"Found potential legal info link: {impressum_link}")

                # Step 9: If Impressum link is found, scrape it
                if impressum_link:
                    scraped_content = scrape_impressum_url(impressum_link)
                    print("scraped_content", scraped_content)
                    if scraped_content:
                        markdown_content = scraped_content["markdown"]
                        company_name, register_number = extract_company_info(markdown_content)

                        # Step 10: Save the data for Excel
                        if company_name and register_number:
                            excel_data.append([title, company_name, register_number])
                        else:
                            logging.error(f"Failed to extract company info from {impressum_link}")
                            raise Exception(f"Failed to extract company info from {impressum_link}")
                    else:
                        logging.error(f"Failed to scrape content from {impressum_link}")
                        raise Exception(f"Failed to scrape content from {impressum_link}")
                else:
                    print("No Impressum link found.")
            else:
                logging.error(f"Failed to crawl {first_link}")
                raise Exception(f"Failed to crawl {first_link}")
        else:
            print(f"No results found for {title}.")
except Exception as e:
    logging.error(f"Error during article processing: {e}")
    raise

# Step 11: Write the extracted data to an Excel file
write_to_excel(excel_data)