"""
ETL script to extract, transform, and load data from "Books to Scrape" website.

The script performs the following steps:
1. Extract:
    - Retrieve HTML pages from all book categories.
    - Retrieve HTML pages from individual product pages.
    - Extract product data (title, price, UPC, different URLs, availability, rating, etc).
2. Transform:
    - Clean and normalize extracted data.
    - Convert relative URLs to absolute URLs.
    - Rename image files using the product UPC as a unique identifier.
3. Load:
    - Save product data into one CSV file per category, in the output/data directory.
    - Dowload and store product images locally in the output/image directory.
    - Add a relative image path ("image_path") column to each CSV.

This version of the script correspond to the Phase 4 of Openclassrooms Project number 2

"""

# THRID-PARTY LIBRARIES
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
from pathlib import Path
from PIL import Image

# HELPERS
# Helper to retrieve data from product information header
def extract_table_value(product_parsed_html, label, url=""):
    header_cell = product_parsed_html.find("th", string=label)
    if header_cell is None:
        print(f"Missing '{label}' for {url}")
        return ""
    row = header_cell.parent
    td_cell = row.find("td")
    if td_cell is None:
        print(f"Missing data_cell for {url}")
        return ""
    return td_cell.get_text(strip=True)

# Helper to retrieve and parse HTML content from URL
def html_parser(url, timeout=30):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        raw_html = response.text
        parsed_html = BeautifulSoup(raw_html, "html.parser")
        return parsed_html
    
    except requests.exceptions.RequestException as exception_type:
        print(f"Connexion error: {exception_type}")
        return Noneq

# Helper to parse: "price", by separating currency and value
def parse_price(raw_price):
    price_currency = "Inconnue"
    for currency_symbol in CURRENCIES:
        if currency_symbol in raw_price:
            price_currency = currency_symbol
            break
    cleaned_price_text = (
        raw_price
            .replace("Â", "")
            .replace(price_currency, "")
            .strip()
    )
    price_value = float(cleaned_price_text)
    parsed_price = {
        "value" : price_value,
        "currency" : price_currency
    }
    return parsed_price

# Helper to extract and clean: "review_rating"
def extract_and_clean_rating(product_page_soup):
    rating_tag = product_page_soup.find("p", class_="star-rating")
    rating_tag_classes = rating_tag["class"]
    rating_text = rating_tag_classes[1]
    rating_mapping = {
        "One" : 1, 
        "Two" : 2, 
        "Three" : 3, 
        "Four" : 4, 
        "Five" : 5
    }
    review_rating = rating_mapping.get(rating_text)
    return review_rating

# Helper to extract and clean: "image_url"
def extract_and_clean_image_url(product_page_soup, product_page_url):
    image_container = product_page_soup.find("div", class_="item active")
    image_tag = image_container.find("img")
    image_relative_url = image_tag["src"]
    image_absolute_url = urljoin(product_page_url, image_relative_url)
    image_url = image_absolute_url
    return image_url

# Helper to extract and clean (and handle missing case): "product_description"
def extract_and_clean_product_description(product_page_soup):
    description_container = product_page_soup.find("div", id="product_description")

    if description_container:
        description_paragraph = description_container.find_next_sibling("p")
        if description_paragraph:
            product_description = description_paragraph.get_text(strip=True)
        else:
            product_description = ""
    else:
        product_description = ""
    return product_description

# Helper to extract and clean: "number_available"
def extract_and_clean_number_available(product_page_soup):
    raw_number_available = extract_table_value(product_page_soup, "Availability")
    digits = []
    for char in raw_number_available:
        if char.isdigit():
            digits.append(char)
    number_available_str = "".join(digits)
    number_available = int(number_available_str)
    return number_available

# Helper to download and validate image
def download_and_validate_images(universal_product_code, image_absolute_url):
    image_filename = f"{universal_product_code}.jpg"
    image_path = images_folder / image_filename
    image_error = ""
    image_download_status = "pending"
    image_path_for_csv = f"images/{image_filename}"

    try:
        response_image_url = requests.get(image_absolute_url, stream=True, timeout=30)
        response_image_url.raise_for_status()
        
        with open(image_path, "wb") as file:
            for chunk in response_image_url.iter_content(8192):
                if chunk:
                    file.write(chunk)
        
        # Validation
        try: 
            with Image.open(image_path) as image:
                image.load()
            image_download_status = "successful"
        except Exception:
            image_download_status = "failed"
            image_error = "Downloaded file is not a valid image"
            image_path_for_csv = "none"
            image_path.unlink(missing_ok=True)

    except requests.exceptions.RequestException as exception_type:
        print(f"Erreur téléchargement image : {exception_type}")
        image_download_status = "failed"  
        image_error = type(exception_type).__name__       
        image_path_for_csv = "none"
        
    return (
        image_error,
        image_download_status,
        image_path_for_csv,
    )

# Helper to extract and clean categories name and url into a list of dictionnaries
def extract_and_clean_categories(homepage_soup):
    categories_sidebar = homepage_soup.find("div", class_="side_categories")
    category_links = categories_sidebar.find_all("a")

    category_index = []
    for link in category_links:
        name = link.get_text(strip=True)
        href = link.get("href")
        if name == "Books":
            continue
        category_index.append({
            "name": name,
            "url": urljoin(homepage_url, href)
        })
    return category_index

# CONSTANTS
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
    "image_local_path",
    "image_download_status",
    "image_error"
]

# MAIN EXECUTION
print(f"Main execution has started")

# Create output folders for CSV files (data folder) and images files (images folder)
output_folder = Path("output")
data_folder = output_folder / "data"
images_folder = output_folder / "images"

data_folder.mkdir(parents=True, exist_ok=True)
images_folder.mkdir(parents=True, exist_ok=True)

# Download and parse "Books to Scrape" website homepage
homepage_url = "https://books.toscrape.com/"
homepage_soup = html_parser(homepage_url)
if homepage_soup is None:
    raise SystemExit("Failed to fetch homepage")

# Extract and clean categories absolute URLs and names
category_index = extract_and_clean_categories(homepage_soup)

# Set an initial number of exported books and images, for later control
total_books = 0
total_images = 0

# Download and parse every category page
for category in category_index:
    category_absolute_url = category["url"]
    category_soup = html_parser(category_absolute_url)
    if category_soup is None:
        raise SystemExit(f"Failed to fetch category: {category["name"]}")

    # Extract category name
    category_name = category["name"]

    # Create a specific csv file per catgeory
    csv_path = data_folder / f"{category_name}.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        # Iterate over each product card to extract product details
        has_next_page = True
        while has_next_page:
            # Extract the URL of product page
            product_cards =  category_soup.find_all("article", class_="product_pod")
            for card in product_cards:
                product_link = card.find("a")
                product_absolute_url = urljoin(category_absolute_url, product_link["href"])

                # Download and parse product page
                product_page_soup = html_parser(product_absolute_url)

                if product_page_soup is None:
                    continue
                
                # Extract "product_page_url"
                product_page_url = product_absolute_url

                # Extract "universal_product_code"
                universal_product_code = extract_table_value(product_page_soup, "UPC")

                # Extract and clean "title"
                raw_title = product_page_soup.title.get_text(strip=True)
                title = raw_title.replace(" | Books to Scrape - Sandbox", "")

                # Extract and normalize price (value + currency) for including and excluding tax
                raw_price_including_tax = extract_table_value(product_page_soup, "Price (incl. tax)")
                price_including_tax = parse_price(raw_price_including_tax)

                raw_price_excluding_tax = extract_table_value(product_page_soup, "Price (excl. tax)")
                price_excluding_tax = parse_price(raw_price_excluding_tax)

                # Extract and clean "number_available"
                number_available = extract_and_clean_number_available(product_page_soup)

                # Extract and clean "product_description", handling missing case
                product_description = ""
                product_description = extract_and_clean_product_description(product_page_soup)

                # Extract and clean each "review_rating"
                review_rating = extract_and_clean_rating(product_page_soup)

                # Extract and clean "image_url" 
                image_absolute_url = extract_and_clean_image_url(product_page_soup, product_page_url)
                image_url = image_absolute_url

                # Download images on local folder ouput/images, and update downloaded images counter
                image_error, image_download_status, image_path_for_csv = download_and_validate_images(universal_product_code, image_absolute_url)

                if image_download_status == "successful":
                    total_images += 1

                # Build one row and write it immediately
                book_data = {
                    "product_page_url": product_page_url,
                    "universal_product_code": universal_product_code,
                    "title": title,
                    "price_including_tax": price_including_tax["value"],
                    "price_excluding_tax": price_excluding_tax["value"],
                    "number_available": number_available,
                    "product_description": product_description,
                    "category": category_name,
                    "review_rating": review_rating,
                    "image_url": image_url,
                    "image_local_path" : image_path_for_csv,
                    "image_download_status" : image_download_status,
                    "image_error" : image_error
                }
                
                writer.writerow(book_data)

                # Update exported books counter
                total_books += 1

            # Find link to next page
            next_page_tag = category_soup.find("li", class_="next")
            if next_page_tag:
                next_page_relative_url = next_page_tag.find("a")["href"]
                category_absolute_url = urljoin(category_absolute_url, next_page_relative_url)
                category_soup = html_parser(category_absolute_url)
            else:
                has_next_page = False

        # Close the csv file for a category
        csv_file.close()

# Controlling the total number of exported books and images
print(f"Total number of exported books: {total_books}")
print(f"Total number of exported images: {total_images}")