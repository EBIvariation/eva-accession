# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pymongo
from urllib.parse import quote_plus


def get_mongo_connection_handle(username: str, password: str, host: str,
                                port: int = 27017, authentication_database: str = "admin") -> pymongo.MongoClient:
    mongo_connection_uri = "mongodb://{0}:{1}@{2}:{3}/{4}".format(username, quote_plus(password), host,
                                                                  port, authentication_database)
    return pymongo.MongoClient(mongo_connection_uri)
