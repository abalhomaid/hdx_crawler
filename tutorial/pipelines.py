import scrapy
import copy
import csv
import json
import pandas as pd
import numpy as np
import os

from scrapy.pipelines.images import FilesPipeline
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter, JsonItemExporter, JsonLinesItemExporter
from scrapy.conf import settings



class MyCsvExporterPipeline(object):

	def __init__(self):
		self.files = {}
		self.tags = []
		self.kwargs = {
			'fields_to_export': ['dataset_id', 'who', 'what', 'when', 'where', 'on_web', 'on_hdx', 'open_data', 'link', 'tag_id', 'tag', 'hxl_id', 'hxl', 'indicator_id', 'indicator', 'quality_id', 'quality'],
			'export_empty_fields': False, 
			'encoding': 'utf-8', 
			'indent': 0,
			'lineterminator': '\\r'
		}

	def open_spider(self, spider):
		file = open('meta_data_test.json', 'w')
		self.files[spider] = file
		self.files[spider].write('[')
		# writer = csv.writer(file, quoting=csv.QUOTE_ALL)
		# writer.writerow(kwargs.get('fields_to_export'))
		# self.exporter = CsvItemExporter(file=file, include_headers_line=True, **kwargs)
		# self.exporter = JsonLinesItemExporter(file=file)
		# self.exporter.start_exporting()


	def close_spider(self, spider):
		file = self.files.pop(spider)
		file.seek(0, os.SEEK_END) # seek to the end of the file
		file.seek(file.tell() - 3, os.SEEK_SET) # go backwards 3 bytes
		file.truncate()
		file.write(']')
		file.close()

		# self.exporter.finish_exporting()
		# file = open('meta_data_test.json', 'rb')
		# df = pd.DataFrame(columns=kwargs['fields_to_export'])
		# for line in file:
		# 	df = df.append([json.loads(line)])
		# df.to_csv('meta_data_test.csv')

		df = pd.read_json('meta_data_test.json', orient='records')
		df = df[self.kwargs['fields_to_export']]
		df.to_csv('meta_data_test.csv')



	def process_item(self, item, spider):
		# dataset dimension - its already taken care of.
        # tag dimension
		for tag in item['tag']:
			row = copy.deepcopy(item)
			row['tag'] = tag

			# tags is stored in a list rather than a file, because existing tag file is missing a lot of education tags
			if(tag in self.tags):
				row['tag_id'] = self.tags.index(tag)
			else:
				self.tags.append(tag)
				row['tag_id'] = self.tags.index(tag)

			# indicator dimension
			for indicator in item['indicator']:
				row['indicator'] = indicator
				row['indicator_id'] = spider.education_indicators.index.get_loc(key=indicator)
				
				# quality dimension
				for quality in item['quality']:
					row['quality'] = quality
					row['quality_id'] = spider.education_quality.index.get_loc(key=quality)
					
					# hxl is only stored if crawler scraps files. If file is not of type csv, or xls, then crawler returns with no item['hxl']
					if 'hxl' in item:
						# hxl dimension
						for hxl in item['hxl']:
							row['hxl'] = hxl
							hxl_location = spider.education_hxls.index.get_loc(key=hxl)
							if(isinstance(hxl_location, np.ndarray)):
								row['hxl_id'] = spider.education_hxls[hxl_location].index[0]
							else:
								row['hxl_id'] = spider.education_hxls.index.get_loc(key=hxl)

							# self.exporter.export_item(row)
							line = json.dumps(dict(row)) + ",\n"
							self.files[spider].write(line)
							# json.dump(dict(row), self.files[spider])
					else:
						# self.exporter.export_item(row)
						line = json.dumps(dict(row)) + ",\n"
						self.files[spider].write(line)
						# json.dump(dict(row), self.files[spider])

		return item