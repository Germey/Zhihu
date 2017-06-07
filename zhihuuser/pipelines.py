# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

from zhihuuser.items import UserItem, QuestionItem, AnswerItem


class ZhihuPipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipeline(object):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        if isinstance(item, UserItem):
            self.db['users'].update({'url_token': item['url_token']}, {'$set': dict(item)}, True)
        elif isinstance(item, QuestionItem):
            self.db['questions'].update({'id': item['id']}, {'$set': dict(item)}, True)
        elif isinstance(item, AnswerItem):
            self.db['answers'].update({'id': item['id']}, {'$set': dict(item)}, True)
        return item
