"""
ETL script to extract, transform, and load data from "Books to Scrape" website.

The script follows a classic ETL pipeline:
1. Download HTML content from the website
2. Parse the HTML structure
3. Extract relevant data
4. Transform and clean the extracted data
5. Save results to a CSV file

The implementation evolves through multiple phases:
- Phase 1: Extract data from a single product page
- Phase 2: Extract data from all products in a given category
- Phase 3: Extract data from all available categories (one CSV per category)
- Phase 4: Download and save product images from each product page

"""

# Third-party libraries for HTTP requests and HTML parsing
import requests
from bs4 import BeautifulSoup

# PHASE 1: EXTRACT DATA FROM A SINGLE PRODUCT PAGE

# Retrieving HTML content from product page
url = "https://books.toscrape.com/catalogue/sharp-objects_997/index.html"
response = requests.get(url)
response_html = response.text

# Save raw HTML locally for analysis
with open("debug_product.html", "w", encoding="utf-8") as f:
    f.write(response_html)

# Parsing the HTML content
soup = BeautifulSoup(response_html, "html.parser")

# Extracting the data
    # Helper to retrieve data from product information header
def extract_table_value(soup, label):
    th = soup.find("th", string=label)
    row = th.parent
    return row.find("td").get_text(strip=True)

    # Extracting product_page_url
product_page_url = response.url

    # Extracting universal_product_code
universal_product_code = extract_table_value(soup, "UPC")

    # Extracting title
title = soup.title.string.get_text(strip=True)

    # Extracting and cleaning price_including_tax and price_excluding_tax as dictionnaries with values and currencies
CURRENCIES = ["£", "€", "$"]
        # Applies to price_including_tax data
raw_price_including_tax = extract_table_value(soup, "Price (incl. tax)")
price_including_tax_currency = "Inconnue"
for currency_symbol in CURRENCIES:
    if currency_symbol in raw_price_including_tax:
        price_including_tax_currency = currency_symbol
        break
cleaned_price_including_tax = (
    raw_price_including_tax
    .replace("Â", "")
    .replace("£", "")
    .strip()
)
price_including_tax_value = float(cleaned_price_including_tax)
price_including_tax = {
    "value" : price_including_tax_value,
    "currency" : price_including_tax_currency,
}
        # Applies to price_excluding_tax data
price_excluding_tax = extract_table_value(soup, "Price (excl. tax)")
price_excluding_tax_currency = "Inconnue"
for currency_symbol in CURRENCIES:
    if currency_symbol in raw_price_excluding_tax:
        price_excluding_tax_currency = currency_symbol
        break
cleaned_price_excluding_tax = (
    raw_price_excluding_tax
    .replace("Â", "")
    .replace("£", "")
    .strip()
)
price_excluding_tax_value = float(cleaned_price_excluding_tax)
price_excluding_tax = {
    "value" : price_excluding_tax_value,
    "currency" : price_excluding_tax_currency,
}

    # Extract and clean number_available in order to keep only the value
raw_number_available = extract_table_value(soup, "Availability")
digits = []
for char in raw_number_available:
    if char.isdigit():
        digits.append(char)
cleaned_number_available = "".join(digits)
number_available = int(cleaned_number_available)

description_header = soup.find("div", id="product_description")
product_description = description_header.find_next_sibling("p").get_text()

ul = soup.find("ul", class_="breadcrumb")
li_items = ul.find_all("li")
third_li = li_items[2]
a = third_li.find("a")
category = a.get_text(strip=True)

p = soup.find("p", class_="star-rating Four")
class_content = p["class"]
review_rating = class_content[1]

div = soup.find("div", class_="item active")
img = div.find("img")
image_url = img["src"]

# Extraction control
print(f"product_page_url : {product_page_url}")
print(f"universal_product_code : {universal_product_code}")
print(f"title : {title}")
print(f"price_including_tax : {price_including_tax}")
print(f"price_excluding_tax : {price_excluding_tax}")
print(f"number_available : {number_available}")
print(f"product_description : {product_description}")
print(f"category : {category}")
print(f"review_rating : {review_rating}")
print(f"image_url : {image_url}")