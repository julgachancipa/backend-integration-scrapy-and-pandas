import os
import numpy as np
import pandas as pd

from sqlalchemy.orm import sessionmaker
from database_setup import engine
from models import BranchProduct, Product, Base

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")
BRANCHES = ['MM', 'RHSM']


def process_csv_files():
    products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|",)
    prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|",)

    return products_df, prices_stock_df


def select_products_df(products_df):
    full_category = ['CATEGORY', 'SUB_CATEGORY', 'SUB_SUB_CATEGORY']
    products_df['FULL_CATEGORY'] = products_df[full_category]\
        .apply(lambda row: '|'.join(row.values.astype(str)), axis=1)

    columns = ['SKU', 'BARCODES', 'NAME', 'DESCRIPTION',
               'IMAGE_URL', 'FULL_CATEGORY', 'BRAND']
    return products_df[columns]


def select_prices_stock_df(prices_stock_df):
    branch_mask = prices_stock_df['BRANCH'].isin(BRANCHES)
    prices_stock_df = prices_stock_df[branch_mask]

    stock_mask = prices_stock_df['STOCK'] > 0
    prices_stock_df = prices_stock_df[stock_mask]

    return prices_stock_df


def products_to_db(products):
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    for key, item in products.items():
        print('Processing:', key, item['NAME'])
        product = (
            session.query(Product)
            .filter_by(store="Richart's", sku=item["SKU"])
            .first()
        )

        if product is None:
            product = Product(store="Richart's", sku=item["SKU"])

        product.barcodes = item["BARCODES"]
        product.brand = item["BRAND"]
        product.name = item["NAME"]
        product.description = item["DESCRIPTION"]
        product.image_url = item["IMAGE_URL"]
        product.category = item["FULL_CATEGORY"]
        product.package = item["DESCRIPTION"].strip(item["NAME"])

        session.add(product)
        session.commit()

        # Check if the BranchProduct already exists
        branch_product = (
            session.query(BranchProduct)
            .filter_by(product=product, branch=item["BRANCH"])
            .first()
        )

        if branch_product is None:
            branch_product = BranchProduct(product=product, branch=item["BRANCH"])

        branch_product.stock = item["STOCK"]
        branch_product.price = item["PRICE"]

        session.add(branch_product)
        session.commit()

    session.close()


if __name__ == "__main__":
    products_df, prices_stock_df = process_csv_files()

    products_df = select_products_df(products_df)
    prices_stock_df = select_prices_stock_df(prices_stock_df)

    full_products_df = pd.merge(products_df, prices_stock_df, on='SKU')
    full_products_df.drop_duplicates(keep=False,inplace=True)

    products = full_products_df.to_dict('index')
    products_to_db(products)
