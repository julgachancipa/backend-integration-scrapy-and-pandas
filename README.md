# Backend integration with Scrapy and Pandas

The purpose of this repository is collecting product information from external sources like websites and files. This test covers both tasks with simple cases:

- Case 1: Scraping a product department at Walmart Canada's website
- Case 2: Processing CSV files to extract clean information

## Installation
I suggest you create a virtual environment:
```
python3 -m venv my_env
```

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the requirements:

```bash
pip install -r requirements.txt
```
Create and initialize the database running:
```
python database_setup.py
```

## Usage

- Case 1:
Run the Scrapy spider
```
scrapy crawl ca_walmart
```

- Case 2: Run the ingestion file in the integrations/richart_wholesale_club folder

```
python ingestion.py
```
