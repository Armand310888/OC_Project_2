"""
ETL script to extract, transform, and load data from "Books to Scrape" website.

The script performs the following steps:
1. Extract:
    - Retrieve HTML homepage.
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
    - Register and save script errors into the output/logs directory, for debug.

This version of the script correspond to the Phase 4 of Openclassrooms Project number 2

"""

# THRID-PARTY LIBRARIES
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
from pathlib import Path
from PIL import Image
import logging

# HELPERS
# Helper to print log error message
def log_error_message(error_message, title= None, url=None):
    logger.error(
        f"Book {title}\n"
        f"Error: {error_message}\n"
        f"Corresponding URL: {url}"
    )

# Helper to retrieve data from product information header
def extract_table_value(product_parsed_html, label, url=None):
    header_cell = product_parsed_html.find("th", string=label)
    if header_cell is None:
        logger.error(f"Missing '{label}' in header_cell for {url}")
        return None
    row = header_cell.parent
    td_cell = row.find("td")
    if td_cell is None:
        logger.error(f"{label}: missing data_cell -> {url}")
        return None
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
        logger.error(f"Connexion error: {exception_type}")
        return None

# Helper to parse: "price", by separating currency and value
def parse_price(raw_price):
    price_currency = "Inconnue"
    if raw_price is None:
        return None
    
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

    if cleaned_price_text == "":
        return None
    
    price_value = float(cleaned_price_text)
    parsed_price = {
        "value" : price_value,
        "currency" : price_currency
    }
    return parsed_price

# Helper to extract and clean: "review_rating"
def extract_and_clean_rating(product_page_soup, title=None, url=None):
    rating_tag = product_page_soup.find("p", class_="star-rating")
    if rating_tag is None:
        log_error_message("missing rating tag", title, url)
        return None
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

    if review_rating is None:
        log_error_message("failed to extract review_rating_data", title, url)
        return None
    
    return review_rating

# Helper to extract and clean: "image_url"
def extract_and_clean_image_url(product_page_soup, product_page_url, title=None, url=None):
    image_container = product_page_soup.find("div", class_="item active")
    if image_container is None:
        log_error_message("image_container missing", title, url)
        return None
    
    image_tag = image_container.find("img")

    if image_tag is None:
        log_error_message("image_tag missing", title, url)
        return None

    image_relative_url = image_tag["src"]
    image_absolute_url = urljoin(product_page_url, image_relative_url)
    image_url = image_absolute_url
    return image_url

# Helper to extract and clean: "product_description"
def extract_and_clean_product_description(product_page_soup, title=None, url=None):
    description_container = product_page_soup.find("div", id="product_description")

    if description_container:
        description_paragraph = description_container.find_next_sibling("p")
        if description_paragraph:
            product_description = description_paragraph.get_text(strip=True)
        else:
            log_error_message("no available product description for this book", title, url)
            product_description = "No available product description for this book"
    else:
        log_error_message("no available product description for this book", title, url)
        product_description = "No available product description for this book"

    return product_description

# Helper to extract and clean: "number_available"
def extract_and_clean_number_available(product_page_soup, title=None, url=None):
    raw_number_available = extract_table_value(product_page_soup, "Availability")
    if raw_number_available is None:
        log_error_message("failed to extract availability data", title, url)
        return None
    digits = []
    for char in raw_number_available:
        if char.isdigit():
            digits.append(char)
    number_available_str = "".join(digits)

    if number_available_str == "":
        log_error_message("failed to extract availability data", title, url)
        return None
    
    number_available = int(number_available_str)
    return number_available

# Helper to download and validate image
def download_and_validate_images(universal_product_code, image_absolute_url, title=None, url=None):
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
        
        # Validation image (Pillow)
        try: 
            with Image.open(image_path) as image:
                image.load()
            image_download_status = "successful"

        except Exception:
            log_error_message("downloaded file is not a valid image", title, url)
            image_download_status = "failed"
            image_error = "Downloaded file is not a valid image"
            image_path_for_csv = "none"
            image_path.unlink(missing_ok=True)

    except requests.exceptions.RequestException as exception_type:
        log_error_message("image download failed", title, url)
        image_download_status = "failed"  
        image_error = type(exception_type).__name__       
        image_path_for_csv = "none"
        image_path.unlink(missing_ok=True)
        
    return (
        image_error,
        image_download_status,
        image_path_for_csv,
    )

# Helper to extract and clean categories name and url into a list of dictionnaries
def extract_and_clean_categories(homepage_soup):
    categories_sidebar = homepage_soup.find("div", class_="side_categories")
    if categories_sidebar is None:
        logger.critical(f"ETL crashed: categories_sidebar is missing on homepage")
        raise SystemExit(1)
    category_links = categories_sidebar.find_all("a")
    if not category_links:
        logger.critical(f"ETL crashed: category_links is missing on homepage")
        raise SystemExit(1)

    category_index = []
    for link in category_links:
        name = link.get_text(strip=True)
        href = link.get("href")
        if not href:
            logger.critical(f"ETL crashed: Missing href for category '{name}'")
            raise SystemExit(1)
        if name == "Books":
            continue
        category_index.append({
            "name": name,
            "url": urljoin(homepage_url, href)
        })
    return category_index

# Helper to setup a logger
def setup_logger(__name__):
    # Create a logger to track bugs and facilitaire debug
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create terminal log handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create logs folder
    logs_folder = output_folder / "logs"
    logs_folder.mkdir(parents=True, exist_ok=True)

    # Create file log handler
    file_handler = logging.FileHandler(logs_folder / "etl.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # Define formatting
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
                                
    # Link formatting to handlers
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Link handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

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

# create ouput folder for logger and CSV files and images
output_folder = Path("output")

# MAIN EXECUTION
# Create a logger for later debug
logger = setup_logger(__name__)

# Create data folder and images folder
data_folder = output_folder / "data"
images_folder = output_folder / "images"

data_folder.mkdir(parents=True, exist_ok=True)
images_folder.mkdir(parents=True, exist_ok=True)

# Download and parse "Books to Scrape" website homepage
homepage_url = "https://books.toscrape.com/"
homepage_soup = html_parser(homepage_url)
if homepage_soup is None:
    logger.critical(f"ETL crashed: Failed to fetch homepage")
    raise SystemExit(1)

# Inform log that main has started
logger.info(f"Main execution has started")

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
        logger.critical(f"ETL crashed: failed to fetch category: {category['name']}")
        raise SystemExit(1)

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
                    logger.error(f"Missing product page for URL: {product_absolute_url}")
                    continue
                
                # Extract "product_page_url"
                product_page_url = product_absolute_url

                # Extract and clean "title"
                raw_title = product_page_soup.title.get_text(strip=True)
                title = raw_title.replace(" | Books to Scrape - Sandbox", "")

                # Extract "universal_product_code"
                universal_product_code = extract_table_value(product_page_soup, "UPC", url=product_absolute_url)
                
                if universal_product_code is None:
                    log_error_message("failed to extract 'UPC' data", title=title, url=product_absolute_url)
                    continue
            
                # Extract and normalize price (value + currency) for including and excluding tax
                raw_price_including_tax = extract_table_value(product_page_soup, "Price (incl. tax)", url=product_absolute_url)
                price_including_tax = parse_price(raw_price_including_tax)

                if price_including_tax is None:
                    log_error_message("failed to parse 'Price incl. tax' data", title=title, url=product_absolute_url)
                    continue

                raw_price_excluding_tax = extract_table_value(product_page_soup, "Price (excl. tax)", url=product_absolute_url)
                price_excluding_tax = parse_price(raw_price_excluding_tax)

                if price_excluding_tax is None:
                    log_error_message("failed to parse 'Price excl. tax' data", title=title, url=product_absolute_url)
                    continue

                # Extract and clean "number_available"
                number_available = extract_and_clean_number_available(product_page_soup, title=title, url=product_absolute_url)

                # Extract and clean "product_description", handling missing case
                product_description = ""
                product_description = extract_and_clean_product_description(product_page_soup, title=title, url=product_absolute_url)

                # Extract and clean each "review_rating"
                review_rating = extract_and_clean_rating(product_page_soup, title=title, url=product_absolute_url)

                # Extract and clean "image_url" 
                image_absolute_url = extract_and_clean_image_url(product_page_soup, product_page_url, title=title, url=product_absolute_url)
                image_url = image_absolute_url

                # Download images on local folder ouput/images, and update downloaded images counter
                image_error, image_download_status, image_path_for_csv = download_and_validate_images(universal_product_code, image_absolute_url, title=title, url=product_absolute_url)

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
logger.info(f"Total number of exported books: {total_books}")
logger.info(f"Total number of exported images: {total_images}")
logger.info(f"Main execution has ended")