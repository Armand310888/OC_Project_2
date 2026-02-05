"""
ETL script to extract, transform, and load data from "Books to Scrape" website.

The script follows a classic ETL pipeline:
1. Download HTML content from website
2. Parse the HTML structure
3. Extract relevant data
4. Transform and clean the extracted data
5. Save results to a CSV file

The implementation evolves throught multiple phases:
- Phase 1: Extract data from a single product page
- Phase 2: Extract data from all products in a given category
- Phase 3: Extract data from all available categories (one CSV per category)
- Phase 4: Download and save product images from each product page

"""

