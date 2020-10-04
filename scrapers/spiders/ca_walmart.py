import scrapy
import json
import re

from scrapers.items import ProductItem


class CaWalmartSpider(scrapy.Spider):
    """
    The CaWalmartSpider extracts the information from the Walmart Canada
    fruit Category depending on the location of the store that is chosen
    """
    name = "ca_walmart"
    allowed_domains = ["walmart.ca"]
    start_urls = ["https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852"]
    page_num = 2
    max_page = 2
    branches = [
        {
            'city': 'Thunder Bay',
            'id': 3124,
            'latitude': '48.412997',
            'longitude': '-89.239717'
        },
        {
            'city': 'Toronto',
            'id': 3106,
            'latitude': '43.656422',
            'longitude': '-79.435567'}
    ]

    def parse(self, response):
        """
        It takes the response from the start urls, looks for the
        url of each product and also take care of the pagination
        """
        products_urls = response.css('.product-link::attr(href)').extract()

        for product_url in products_urls:
            yield response.follow(product_url, callback=self.parse_product,
                                  cb_kwargs={'url': product_url})

        next_pag = 'https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852/page-'\
                   + str(self.page_num)
        if self.page_num <= self.max_page:
            yield response.follow(next_pag, callback=self.parse)

    def parse_product(self, response, url):
        """
        It searches for the information we need to extract from
        each product to finally verify its availability in each store
        """
        preloaded_state = re.findall(r'(\{.*\})', response.xpath('/html/body/script[1]').extract()[0])
        preloaded_state = json.loads(preloaded_state[0])

        item = ProductItem()

        item['store'] = 'Walmart'
        item['url'] = self.allowed_domains[0] + url
        item['sku'] = preloaded_state['product']['activeSkuId']

        item_skus = preloaded_state['entities']['skus'][item['sku']]
        item['barcodes'] = ','.join(item_skus['upc'])
        item['brand'] = item_skus['brand']['name']
        item['name'] = item_skus['name']
        item['description'] = item_skus['longDescription']
        item['package'] = item_skus['description']
        item['image_url'] = item_skus['images'][0]['enlarged']['url']

        categories = []
        for category in preloaded_state['product']['item']['primaryCategories'][0]['hierarchy']:
            categories.append(category['displayName']['en'])
        categories.reverse()
        item['category'] = '>'.join(categories)

        for branch in self.branches:
            branch_url = 'https://www.walmart.ca/api/product-page/find-in-store' \
                         '?latitude={}&longitude={}&lang=en&upc={}'\
                .format(branch['latitude'], branch['longitude'], item_skus['upc'][0])

            yield response.follow(branch_url, callback=self.parse_branch,
                                  cb_kwargs={'item': item, 'branch': branch['id']})

    def parse_branch(self, response, item, branch):
        """
        It checks the availability and price of the product in the store
        :param item: product item
        :param branch: store id
        """
        stock = 0
        price = 0

        stores_info = json.loads(response.text)['info']
        for store_info in stores_info:
            if store_info['id'] == branch:
                stock = store_info['availableToSellQty']
                if stock != 0:
                    price = store_info['sellPrice']

        item['branch'] = str(branch)
        item['stock'] = stock
        item['price'] = price
        yield item
