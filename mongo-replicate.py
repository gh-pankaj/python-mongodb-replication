import json
from pymongo import MongoClient
import nested_lookup
import os
from faker import Faker
from itertools import islice
import time


SOURCE_MONGO_URI = os.getenv('SOURCE_MONGO_URI')
TARGET_MONGO_URI = os.getenv('TARGET_MONGO_URI')
REPLICATION_CONFIG_FILE = os.getenv('REPLICATION_CONFIG_FILE')

with open(REPLICATION_CONFIG_FILE, 'r') as f:
    replication_config = json.load(f)


def get_fake_value(original_value, type, _faker):
    if not hasattr(_faker, type):
        raise NameError("type={} not allowed".format(type))
    # if the original_value is not hashable (e.g. dict or list) return the original value.
    if '__hash__' not in dir(original_value):
        return original_value
    # get method from _faker which has a given name
    faker_function = getattr(_faker, type)
    # seed _faker with the numeric hash of the original value
    _faker.seed_instance(hash(original_value))
    return faker_function()


def anonymise_data(data, fields_to_anonymise, _faker):
    for field in fields_to_anonymise:
        nested_lookup.nested_alter(data, field['field_name'], callback_function=get_fake_value, function_parameters=[
                                   field['field_type'], _faker], in_place=True)


source_client = MongoClient(SOURCE_MONGO_URI)
source_collection = source_client[replication_config['source_db']
                                  ][replication_config['source_collection']]


target_client = MongoClient(TARGET_MONGO_URI)
target_collection = target_client[replication_config['target_db']
                                  ][replication_config['target_collection']]

_faker = Faker()

with open(replication_config['dump_filepath'], 'w') as dumpfile:
    for record in source_collection.find(replication_config['filter'], replication_config['projection']):
        anonymise_data(
            record, replication_config['fields_to_anonymise'], _faker)
        dumpfile.write(json.dumps(record))
        dumpfile.write('\n')

_start = time.time()
with open(replication_config['dump_filepath'], 'r') as dumpfile:
    while True:
        next_batch = list(islice(dumpfile, replication_config['chunksize']))
        if not next_batch:
            break
        target_collection.insert_many(
            [json.loads(line) for line in next_batch])
_end = time.time()
print(_end-_start)
