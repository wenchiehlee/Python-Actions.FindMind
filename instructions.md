# Instructions for "FindMind-fetch_and_save_stock_data.py"

0. Still draft and under working
1. Use the `requests` library to make API calls to the FinMind API to fetch stock data.
2. Ensure that API responses are parsed correctly as JSON and handle any HTTP or API errors gracefully.
3. Save the retrieved data in CSV format with appropriate column headers.
4. Use the `pandas` library for handling CSV data to validate and clean auction data.
5. Implement functions to download and save Google Sheets data as a CSV file from a public URL.
6. Validate the content of downloaded or saved files to ensure integrity.
7. Use environment variables to securely load API tokens with the `dotenv` library.
8. Parse and format dates using the `datetime` module to match API requirements.
9. For each stock ID, generate CSV filenames dynamically based on the stock ID and date range.
10. Handle edge cases such as missing data, invalid rows, or errors during processing.
11. Write comprehensive log messages to inform the user of successes, failures, or skipped rows.
12. Organize the script into clear, reusable functions for modularity and readability.
13. Ensure that CSV files are encoded in UTF-8 for compatibility.
14. Provide a `main` function to orchestrate the flow of the script, including:
    - Loading the API token from the environment.
    - Downloading Google Sheets data.
    - Validating and processing the CSV file.
    - Fetching stock data for each row in the CSV and saving it.
15. Include error handling at every critical step to prevent script crashes and to log relevant information.
16. Adhere to Python's best practices and PEP8 guidelines.
