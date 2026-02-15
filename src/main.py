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
from urllib.parse import urljoin
import csv

# PHASE 1: EXTRACT DATA FROM A SINGLE PRODUCT PAGE

# Retrieving HTML content from product page
url = "https://books.toscrape.com/catalogue/sharp-objects_997/index.html"
response = requests.get(url)
raw_html = response.text

# Save raw HTML locally for analysis
with open("debug_product.html", "w", encoding="utf-8") as file:
    file.write(raw_html)

# Parsing the HTML content
parsed_html = BeautifulSoup(raw_html, "html.parser")

# Extracting the data
    # Helper to retrieve data from product information header
def extract_table_value(parsed_html, label):
    th = parsed_html.find("th", string=label)
    row = th.parent
    return row.find("td").get_text(strip=True)

    # Extracting "product_page_url"
product_page_url = response.url

    # Extracting "universal_product_code"
universal_product_code = extract_table_value(parsed_html, "UPC")

    # Extracting "title"
raw_title = parsed_html.title.get_text(strip=True)
title = raw_title.replace(" | Books to Scrape - Sandbox", "")

    # Extracting and cleaning "price_including_tax" and "price_excluding_tax" as dictionnaries with values and currencies
CURRENCIES = ["£", "€", "$"]
        # Applying to "price_including_tax data"
raw_price_including_tax = extract_table_value(parsed_html, "Price (incl. tax)")
price_including_tax_currency = "Inconnue"
for currency_symbol in CURRENCIES:
    if currency_symbol in raw_price_including_tax:
        price_including_tax_currency = currency_symbol
        break
cleaned_price_including_tax_text = (
    raw_price_including_tax
    .replace("Â", "")
    .replace("£", "")
    .strip()
)
price_including_tax_value = float(cleaned_price_including_tax_text)
price_including_tax = {
    "value" : price_including_tax_value,
    "currency" : price_including_tax_currency,
}
        # Applying to "price_excluding_tax data"
raw_price_excluding_tax = extract_table_value(parsed_html, "Price (excl. tax)")
price_excluding_tax_currency = "Inconnue"
for currency_symbol in CURRENCIES:
    if currency_symbol in raw_price_excluding_tax:
        price_excluding_tax_currency = currency_symbol
        break
cleaned_price_excluding_tax_text = (
    raw_price_excluding_tax
    .replace("Â", "")
    .replace("£", "")
    .strip()
)
price_excluding_tax_value = float(cleaned_price_excluding_tax_text)
price_excluding_tax = {
    "value" : price_excluding_tax_value,
    "currency" : price_excluding_tax_currency,
}

    # Extract and clean "number_available" in order to keep only the value
raw_number_available = extract_table_value(parsed_html, "Availability")
digits = []
for char in raw_number_available:
    if char.isdigit():
        digits.append(char)
cleaned_number_available = "".join(digits)
number_available = int(cleaned_number_available)

    # Extract and clean "product_description"
description_header = parsed_html.find("div", id="product_description")
product_description = description_header.find_next_sibling("p").get_text()

    # Extract and clean "category"
ul = parsed_html.find("ul", class_="breadcrumb")
li_items = ul.find_all("li")
third_li = li_items[2]
a = third_li.find("a")
category = a.get_text(strip=True)

    # Extract and clean "review_rating"
p = parsed_html.find("p", class_="star-rating")
class_content = p["class"]
review_rating_text = class_content[1]
review_number_mapping = {
    "One" : 1, 
    "Two" : 2, 
    "Three" : 3, 
    "Four" : 4, 
    "Five" : 5
    }
review_rating = review_number_mapping.get(review_rating_text)

    # Extract and clean "image_url" 
div = parsed_html.find("div", class_="item active")
img = div.find("img")
image_relative_url = img["src"]
image_url = urljoin(product_page_url, image_relative_url)

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

# Loading the data into a CSV file
book_data = {
    "product_page_url" : product_page_url,
    "universal_product_code" : universal_product_code,
    "title" : title,
    "price_including_tax" : price_including_tax["value"],
    "price_excluding_tax" : price_excluding_tax["value"],
    "number_available" : number_available,
    "product_description" : product_description,
    "category" : category,
    "review_rating" : review_rating,
    "image_url" : image_url
}

with open("book_data.csv", "w", encoding="utf-8", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=book_data.keys())
    writer.writeheader()
    writer.writerow(book_data)