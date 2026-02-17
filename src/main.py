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
# PHASE 3: EXTRACT DATA FROM EVERY PRODUCT FROM EVERY CATEGORIES

# Third-party libraries
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv

# Helper to retrieve data from product information header
def extract_table_value(product_parsed_html, label):
    th = product_parsed_html.find("th", string=label)
    row = th.parent
    return row.find("td").get_text(strip=True)

# Helper to retieve and parse HTML content from URL
def html_parser(url):
    response = requests.get(url)
    raw_html = response.text
    return BeautifulSoup(raw_html, "html.parser")

# Create a list of currencies
CURRENCIES = ["£", "€", "$"]

# Define a fixed column order for the CSV
fieldnames = [
    "product_page_url",
    "universal_product_code",
    "title",
    "price_including_tax",
    "price_excluding_tax",
    "number_available",
    "product_description",
    "category",
    "review_rating",
    "image_url",
]

# Dowload and parse "Books to Scrape" website homepage
homepage_url = "https://books.toscrape.com/"
homepage_parsed = html_parser(homepage_url)

# Extracting categories absolute URLs
categories_parent = homepage_parsed.find("div", class_="side_categories")
categories_content = categories_parent.find_all("a")
categories_absolute_urls = [
    urljoin(homepage_url, relative_url["href"])
    for relative_url in categories_content
]
categories_absolute_urls = []
for a in categories_content:
    name = a.get_text(strip=True)
    href = a.get("href")
    if name == "Books":
        continue
    categories_absolute_urls.append(urljoin(homepage_url, href))

# Setting an initial number of exported books, for later control
total_books = 0

# Download and parse every category page
for category_absolute_url in categories_absolute_urls:
    category_parsed = html_parser(category_absolute_url)

    # Extract category name
    category_name = (
        category_parsed
        .find("li", class_="active")
        .get_text(strip=True)
    )

    # Create a specific csv file per catgeory
    csv_name = f"{category_name}.csv"
    file = open(csv_name, "w", encoding="utf-8", newline="")
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    has_next_page = True
    while has_next_page:
        # Extracting the URL of each product page
        products_blocks =  category_parsed.find_all("article", class_="product_pod")
        for product_block in products_blocks:
            product_content = product_block.find("a")
            product_absolute_url = urljoin(category_absolute_url, product_content["href"])

            # Dowload and parse each product page
            product_page_parsed = html_parser(product_absolute_url)
            
            # Extracting each "product_page_url"
            product_page_url = product_absolute_url

            # Extracting each "universal_product_code"
            universal_product_code = extract_table_value(product_page_parsed, "UPC")

            # Extracting and cleaning each "title"
            raw_title = product_page_parsed.title.get_text(strip=True)
            title = raw_title.replace(" | Books to Scrape - Sandbox", "")

            # Extracting and cleaning each "price_including_tax" and "price_excluding_tax" as dictionnaries with values and currencies
                # Applying to "price_including_tax data"
            raw_price_including_tax = extract_table_value(product_page_parsed, "Price (incl. tax)")
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
            raw_price_excluding_tax = extract_table_value(product_page_parsed, "Price (excl. tax)")
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

            # Extract and clean each "number_available" in order to keep only the value
            raw_number_available = extract_table_value(product_page_parsed, "Availability")
            digits = []
            for char in raw_number_available:
                if char.isdigit():
                    digits.append(char)
            cleaned_number_available = "".join(digits)
            number_available = int(cleaned_number_available)

            # Extract and clean each "product_description"
            description_header = product_page_parsed.find("div", id="product_description")

            if description_header:
                next_p = description_header.find_next_sibling("p")
                if next_p:
                    product_description = next_p.get_text(strip=True)
                else:
                    product_description = ""
            else:
                product_content = ""

            # Extract and clean each "category"
            ul = product_page_parsed.find("ul", class_="breadcrumb")
            li_items = ul.find_all("li")
            third_li = li_items[2]
            a = third_li.find("a")
            category = a.get_text(strip=True)

            # Extract and clean each "review_rating"
            p = product_page_parsed.find("p", class_="star-rating")
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

            # Extract and clean each "image_url" 
            div = product_page_parsed.find("div", class_="item active")
            img = div.find("img")
            image_relative_url = img["src"]
            image_url = urljoin(product_page_url, image_relative_url)

            # Build one row and write it immediately
            book_data = {
                "product_page_url": product_page_url,
                "universal_product_code": universal_product_code,
                "title": title,
                "price_including_tax": price_including_tax["value"],
                "price_excluding_tax": price_excluding_tax["value"],
                "number_available": number_available,
                "product_description": product_description,
                "category": category,
                "review_rating": review_rating,
                "image_url": image_url,
            }
            writer.writerow(book_data)

            # Adding 1 to the number of exported books after each iteration
            total_books += 1


        # Find link to next page
        next_page_data = category_parsed.find("li", class_="next")
        if next_page_data:
            next_page_index = next_page_data.find("a")["href"]
            category_absolute_url = urljoin(category_absolute_url, next_page_index)
            category_parsed = html_parser(category_absolute_url)
        else:
            has_next_page = False

    # CLosing the csv file for a category
    file.close()

# Controlling the total number of exported books
print(f"Total number of exported books: {total_books}")