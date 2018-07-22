import scrapy
import numpy as np
import pandas as pd
import io
import xlrd
import logging
import copy

from scrapy.http import TextResponse
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from io import BytesIO


# Education meta data fields
class MetadataItem(scrapy.Item):
    
    # dataset dimension
    dataset_id = scrapy.Field()
    who = scrapy.Field()
    what = scrapy.Field()
    when = scrapy.Field()
    where = scrapy.Field()
    on_web = scrapy.Field()
    on_hdx = scrapy.Field()
    open_data = scrapy.Field()
    link = scrapy.Field()
    url = scrapy.Field()

    # tag dimension
    tag_id = scrapy.Field()
    tag = scrapy.Field()

    # hxl dimension
    hxl_id = scrapy.Field()
    hxl = scrapy.Field()

    # indicator dimension
    indicator_id = scrapy.Field()
    indicator = scrapy.Field()

    # quality dimension
    quality_id = scrapy.Field()
    quality = scrapy.Field()

class HDXSpider(scrapy.Spider):
    name = 'hdx'
    hdx_css_selectors_path = '../resources/hdx_css_selectors.csv'
    education_tags_path = '../resources/education_tags.csv'
    education_hxls_path = '../resources/education_hxls.csv'
    education_indicators_path = '../resources/education_indicator_description.csv'
    quality_path = '../resources/quality.csv'
    
    css_selectors = pd.read_csv(hdx_css_selectors_path, header=0, index_col=0, quotechar="'")
    education_tags = pd.read_csv(education_tags_path, header=0, index_col=0)
    education_indicators = pd.read_csv(education_indicators_path, header=0, index_col=0, quotechar='"')
    education_hxls = pd.read_csv(education_hxls_path, header=0, index_col=1)
    education_quality = pd.read_csv(quality_path, header=0, index_col=0, quotechar="'")

    # lower case tags for comparison reasons
    education_tags.index = education_tags.index.str.lower()

   

    def start_requests(self):
        # urls = ['https://data.humdata.org/search?q=education']
        urls = ['https://data.humdata.org/search?groups=irq&groups=ssd&groups=syr&groups=yem&q=education&ext_page_size=25&sort=score+desc%2C+metadata_modified+desc']
        for url in urls:
            request = scrapy.Request(url=url, callback=self.parse)
            request.meta['url'] = 'https://data.humdata.org'
            request.meta['num_datasets_scraped'] = 0
            request.meta['count'] = 0
            yield request

    # parses HDX search result page
    def parse(self, response):
        update_date_regex = '(?!Updated )\\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May?|June?|July?|August?|Sep(?:temeber)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?) \d{1,2}, \d{4}'
        who = response.css(self.css_selectors.loc['who', 'css'])
        what = response.css(self.css_selectors.loc['what', 'css'])
        when = response.css(self.css_selectors.loc['when', 'css']).re(update_date_regex)
        dataset_links = response.css(self.css_selectors.loc['dataset_links', 'css']).extract()
        dataset_links = dataset_links
        # follow links to dataset pages
        for idx, href in enumerate(dataset_links):
            print(idx, href)
            request = scrapy.Request(response.meta['url'] + href, callback=self.parse_dataset)

            item = MetadataItem()
            item['what'] = what.extract()[idx]
            item['when'] = when[idx]
            item['who'] = who.extract()[idx].strip()
            item['dataset_id'] = idx + response.meta['num_datasets_scraped']
            item['url'] = response.meta['url']

            request.meta['item'] = item
            yield request

        # follow pagination link
        next_page_href = response.css(self.css_selectors.loc['next_page_href', 'css'])
        if response.meta['count'] < 1:
            logging.info('Reached page %s', str(response.meta['count'] + 1))
            meta = {
                'count': response.meta['count'] + 1,
                'url': response.meta['url'],
                'num_datasets_scraped': idx + response.meta['num_datasets_scraped'] + 1
            }
            if(next_page_href):
                yield response.follow(next_page_href.extract()[-1], self.parse, meta=meta)

    # parses HDX dataset page
    def parse_dataset(self, response):
        where = response.css(self.css_selectors.loc['where', 'css']).extract_first()
        tags = response.css(self.css_selectors.loc['tags', 'css']).extract()
        preview_button = response.css(self.css_selectors.loc['preview_button', 'css']).extract_first()
        dataset_download_link = None

        # if there exists a preview button, then the download link is next to it
        if(preview_button and (preview_button.strip() == 'Preview')):
            dataset_download_link = response.css(self.css_selectors.loc['dataset_download_link', 'css']).extract()
            dataset_download_link = dataset_download_link[1]
        else:
            dataset_download_link = response.css(self.css_selectors.loc['dataset_download_link', 'css']).extract_first()


        # map tags to education indicator
        education_indicator = []
        quality_indicator = []
        
        # search tag in and see if it maps to education or quality indicator. If so, add it
        for tag in tags:
            tag = tag.strip().lower()
            # row = self.education_tags[self.education_tags[self.education_tags.columns[0]].str.contains(tag)].values
            row = self.education_tags[self.education_tags.index.str.contains(tag)].values

            if(row.size != 0):
                row = row.flatten()
                education_indicator.append(row[0])
                quality_indicator.append(row[1])

        pd.isnull(np.array([np.nan, 0], dtype=object))


        education_indicator = pd.Series(education_indicator, dtype=object)
        education_indicator = education_indicator[pd.notnull(education_indicator)]
        education_indicator = pd.unique(education_indicator)
        
        quality_indicator = pd.Series(quality_indicator, dtype=object)
        quality_indicator = quality_indicator[pd.notnull(quality_indicator)]
        quality_indicator = pd.unique(quality_indicator)

        item = response.meta['item']
        item['where'] = where
        item['indicator'] = education_indicator
        item['quality'] = quality_indicator
        item['on_web'] = True
        item['on_hdx'] = True
        item['open_data'] = True
        item['tag'] = tags
        item['link'] = response.url

        # if link is csv or xls or xlsx, download and scan
        if(dataset_download_link and (dataset_download_link.endswith('.csv') or dataset_download_link.endswith('.xls') or dataset_download_link.endswith('.xlsx'))):
            file_url = dataset_download_link
            meta = {
                'file_type': dataset_download_link[-4:],
                'item': item
            }

            yield response.follow(file_url, callback=self.parse_file, meta=meta)

        # elif zip file, download it and check if the files contain xlsx to scan them. See Nepal https://data.humdata.org/dataset/nepal-census-2011-district-profiles-education

        else:
            # dataset dimension
            # tag dimension
            # hxl dimension
            # indicator dimension
            # quality dimension


            yield item

    # parses downloaded dataset file
    # 1- read the file education_hxl.csv that maps HXL to education indicator. 
    # 2- read the downloaded file. 
    # 3- if the downloaded file contains education key words that are in education_hxl.csv, add the key word as an indicator form education_hxl.csv  
    def parse_file(self, response):
        education_hxls = self.education_hxls.copy(deep=True)
        education_hxls.index = education_hxls.index.str.replace('$', '')
        education_hxls.index = education_hxls.index.str.replace('+', '')
        education_hxls.index = education_hxls.index.str.replace('#', '')

        # if the response is a csv file. Otherwise, it is a xls file
        if(hasattr(response, 'encoding')):
            data = response.body.decode(response.encoding)
            data = io.StringIO(data)
            df = pd.read_csv(data)
            key_words = df.columns
        else:
            df = pd.read_excel(BytesIO(response.body), engine='xlrd', dtype=object)
            key_words = df.values.flatten()
            key_words = key_words.astype(str)

        # removes unnecassry key words (numbers, duplicates, etc) 
        key_words = self.clean_key_words(key_words)
        
        contained_in = np.array([])
        for word in key_words:
            # indices where word is in HXL
            indices = np.argwhere(education_hxls.index.str.contains('\\b' + word + '\\b', case=False, regex=True)).flatten()
            contained_in = np.append(contained_in, indices)

        contained_in = np.unique(contained_in)
        education_hxls = education_hxls.iloc[contained_in]
        positions_0 = education_hxls[education_hxls.columns[2]].notnull()
        positions_1 = education_hxls[education_hxls.columns[3]].notnull()

        hxls = self.education_hxls.iloc[contained_in].index.values
        updated_for = (education_hxls[positions_0])[education_hxls.columns[2]]
        updated_for = list(updated_for.apply(lambda x: x.split(','))) # hxl could map to multiple indicators, thus, split
        updated_for = [i for sublist in updated_for for i in sublist] # flatten

        updated_quality = (education_hxls[positions_1])[education_hxls.columns[3]]
        updated_quality = list(updated_quality.apply(lambda x: x.split(','))) # hxl could map to multiple qualities, thus, split
        updated_quality = [i for sublist in updated_quality for i in sublist] # flatten

        item = response.meta['item']

        updated_for = np.unique(np.append(item['indicator'], updated_for))
        updated_quality = np.unique(np.append(item['quality'], updated_quality))

        
        item['indicator'] = updated_for
        item['quality'] = updated_quality
        item['link'] = response.url
        item['hxl'] = hxls

        yield item

    def clean_key_words(self, key_words):
        
        # split string into keywords series
        key_words = [x.split() for x in key_words]
        key_words = np.hstack(key_words)
        key_words = pd.Series(key_words, dtype=object)

        # remove null words
        key_words = key_words[pd.notnull(key_words)]

        # remove numeric words
        key_words = key_words[~key_words.str.isnumeric()]

        # remove words that are not alphanumeric
        key_words = key_words[key_words.str.isalnum()]

        # remove char that are not alphanumeric for every word in key words
        key_words = pd.Series([''.join(e for e in string if e.isalnum()) for string in key_words])

        # remove duplicate key words (keep the first instance)
        key_words = pd.unique(key_words)
        return key_words

# Scrapy COMMANDS CHEATSHEAT
# 0- scrapy crawl quotes to run spider
# 1- scrapy shell "http://quotes.toscrape.com" returns response object
# 2- response.css("div.quote") returns SelectorList of div='quote'
# 3- response.css("div.quote").extract_first()  reuturns the first result
# 4- response.css('title::text').re(r'Quotes.*') extracts using regex, ::text selects text element directly from title
# 5- response.css('li.next a').extract_first()
# 6- response.css('li.next a::attr(href)').extract_first()
# 6- scrapy crawl hdx -o meta.json stores scraped data. Make sure to delete the file quotes.json before running (or use json line format)
# 7- Scrapy filters out duplicated requests to URLs already visited
# 8- Request.meta can have builtin keys 


# TODO: plot all combination of dimension 5*4 pivot table