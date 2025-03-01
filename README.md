OLX Apartment Scraper and Telegram Notifier
This Python script scrapes new apartment listings from OLX (a popular classifieds website) based on specific filters for the city of Łódź, Poland. The script then stores the data in a CSV file and sends notifications to a Telegram chat whenever new listings are found.

Features
Scrapes apartments: Filters apartment listings by number of rooms (2 and 3), square footage, and price range from 100,000 to 350,000 PLN.
CSV storage: Saves new listings in a CSV file to avoid duplicates and for future reference.
Telegram notifications: Sends new listings as messages to predefined Telegram chat IDs in HTML format, including a link to the listing.
Requirements
Before running the script, make sure you have the following installed:

Python 3.x
requests library for HTTP requests
beautifulsoup4 for parsing HTML
lxml for faster HTML parsing

How it Works
Scraping Data: The script sends an HTTP request to OLX, fetching listings for apartments in Łódź that meet the criteria.
Parsing Listings: BeautifulSoup is used to parse the HTML and extract details such as the apartment name, price, size, location, and link.
Storing Listings: The new listings are compared against a previously saved CSV file. If there are new listings, they are appended to the file.
Sending Notifications: For each new listing, the script sends a formatted Telegram message to each specified chat ID, providing details about the apartment and a link to the listing.
