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
# PHASE 2: EXTRACT DATA FROM ALL PRODUCT IN A GIVEN CATEGORY

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

# Open the CSV, write the header, then write one row per product
with open("book_data.csv", "w", encoding="utf-8", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    # Retrieving HTML content from a category of products pages
    category_url = "https://books.toscrape.com/catalogue/category/books/mystery_3/index.html"

    current_page_url = category_url

    has_next_page = True

    # Retrieving and parsing every product page from a category. And then extracting, transforming, and loading the data of each product page
    while has_next_page:
        # Download and parse current page
        category_response = requests.get(current_page_url)
        category_raw_html = category_response.text
        category_parsed_html = BeautifulSoup(category_raw_html, "html.parser")

        # Extracting the URL of each product in current page
        products_blocks =  category_parsed_html.find_all("article", class_="product_pod")
        for product_block in products_blocks:
            data = product_block.find("a")
            product_relative_url = data["href"]
            product_url = urljoin(current_page_url, product_relative_url)

            # Dowload and parse each product page
            product_response = requests.get(product_url)
            product_raw_html = product_response.text
            product_parsed_html = BeautifulSoup(product_raw_html, "html.parser")
            
            # Extracting each "product_page_url"
            product_page_url = product_url

            # Extracting each "universal_product_code"
            universal_product_code = extract_table_value(product_parsed_html, "UPC")

            # Extracting each "title"
            raw_title = product_parsed_html.title.get_text(strip=True)
            title = raw_title.replace(" | Books to Scrape - Sandbox", "")

            # Extracting and cleaning each "price_including_tax" and "price_excluding_tax" as dictionnaries with values and currencies
                # Applying to "price_including_tax data"
            raw_price_including_tax = extract_table_value(product_parsed_html, "Price (incl. tax)")
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
            raw_price_excluding_tax = extract_table_value(product_parsed_html, "Price (excl. tax)")
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
            raw_number_available = extract_table_value(product_parsed_html, "Availability")
            digits = []
            for char in raw_number_available:
                if char.isdigit():
                    digits.append(char)
            cleaned_number_available = "".join(digits)
            number_available = int(cleaned_number_available)

            # Extract and clean each "product_description"
            description_header = product_parsed_html.find("div", id="product_description")
            product_description = description_header.find_next_sibling("p").get_text()

            # Extract and clean each "category"
            ul = product_parsed_html.find("ul", class_="breadcrumb")
            li_items = ul.find_all("li")
            third_li = li_items[2]
            a = third_li.find("a")
            category = a.get_text(strip=True)

            # Extract and clean each "review_rating"
            p = product_parsed_html.find("p", class_="star-rating")
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
            div = product_parsed_html.find("div", class_="item active")
            img = div.find("img")
            image_relative_url = img["src"]
            image_url = urljoin(product_page_url, image_relative_url)

            # Script control
            print(
                f"""
            ------------------------------
            Product URL: {product_page_url}
            UPC: {universal_product_code}
            Title: {title}
            Price (incl. tax): {price_including_tax['value']} {price_including_tax['currency']}
            Price (excl. tax): {price_excluding_tax['value']} {price_excluding_tax['currency']}
            Available: {number_available}
            Category: {category}
            Rating: {review_rating}
            Image URL: {image_url}
            ------------------------------
            """
            )

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

        # Find link to next page
        next_page_data = category_parsed_html.find("li", class_="next")
        if next_page_data:
            next_page_index = next_page_data.find("a")["href"]
            current_page_url = urljoin(current_page_url, next_page_index)
        else:
            has_next_page = False

