import copy
import json
from pymongo import DESCENDING, UpdateOne
from bson import json_util
import elasticsearch.helpers

class Loader:
    def __init__(self, ext_con, com_con, settings):
        self.ext_con = ext_con
        self.com_con = com_con
        self.settings = settings
        self.resume_point = None

    def read_data(self):
        try:
            self.resume_point = self.find_resume_point()
            if 'Index' in self.ext_con:
                body_ = {
                    "query": {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "mongoes_id"
                                    }
                                }
                            }
                        }
                    }
                page_ = self.ext_con['Client'].search(
                            index = self.ext_con['Index'], 
                            size = self.settings['FREQUENCY'], 
                            body=body_
                        )
                hits_ = page_['hits']['hits']
                return hits_
                # return {"status": "succeeded"}
            else:
                pipeline = {
                    "mongoes_id": {
                        "$exists": False
                        }
                    }
                # Mongo Stop Point
                return list(self.ext_con['Collection'].find(pipeline).limit(self.settings['FREQUENCY']))
        except Exception as e:
            # TBD: Handle Exception
            hits_ = []
        return hits_

    def tag_documents(self, hits_):
        sour_trace = []
        dest_trace = []

        if 'Index' in self.ext_con:
            for each_hit in hits_:
                dest_trace.append(each_hit['_source'])
                self.resume_point+=1
                dest_trace[-1]['mongoes_id'] = self.resume_point
                sour_trace.append({
                    "_op_type": "update", 
                    "_index": each_hit["_index"],
                    "_id": each_hit["_id"], 
                    'doc': {
                        "mongoes_id": self.resume_point
                    }
                })
        else:
            for each_hit in hits_:
                each_hit = json.loads(json_util.dumps(each_hit))
                self.resume_point+=1
                each_hit['mongoes_id'] = self.resume_point
                sour_trace.append({"_id": each_hit["_id"]['$oid'], "mongoes_id": self.resume_point})
                # each_hit['_id'] = self.resume_point
                each_hit.pop('_id')
                dest_trace.append({
                    "_op_type": "insert", 
                    '_index': self.com_con['Index'],
                    '_id': self.resume_point,
                    'doc': each_hit
                })
        return sour_trace, dest_trace

    def write_data(self, list_):
        if list_ == []:
            return []
        try:
            ext_trace, com_trace = self.tag_documents(list_)
            if 'Index' in self.ext_con:
                elasticsearch.helpers.bulk(self.ext_con['Client'], ext_trace)
                if com_trace != []:
                    self.com_con['Collection'].insert_many(com_trace, ordered=False)
                # TBD: Handle successful write
            else:
                bulk = []
                for doc in ext_trace:
                    bulk.append(
                        UpdateOne(
                            {'_id': doc['_id']},
                            {'$set': {
                                'mongoes_id': doc['mongoes_id']
                                }
                            }))
                bulk_write_response = self.ext_con['Collection'].bulk_write(bulk)
                print(com_trace)
                elasticsearch.helpers.bulk(self.com_con['Client'], com_trace)
        except Exception as e:
            print(e)
            return {}

    def validate_conf(self):
        try:
            self.ext_connection()
        except Exception as e:
            return 'Something is wrong with the connection. Check your connection/config file'

        try:
            self.com_connection()
        except Exception as e:
            return 'Something is wrong with the connection. Check your connection/config file'

        return None
        # TBD: Need to include other test validations right here

    def find_resume_point(self):
        try:
            if 'Index' in self.ext_con:
                # ES's Resume Point
                pipeline = {
                    "aggs" : {
                        "max_mongoes_id" : {
                            "max" : {
                                "field" : "mongoes_id"
                                }
                            }
                        }
                    }
                res_point = self.ext_con['Client'].search(
                                    index = self.ext_con['Index'], 
                                    size = 0, 
                                    body = pipeline)
                return 0 \
                    if res_point['aggregations']['max_mongoes_id']['value'] == None \
                    else int(res_point['aggregations']['max_mongoes_id']['value'])
            else:
                # Mongo's Resume Point
                return int(self.com_con.find().sort("_id", DESCENDING).limit(1)[0]["_id"])
        except Exception as e:
            return 0

    def find_remaining_count(self):
        try:
            if 'Index' in self.ext_con:
                pipeline = {
                    "query": {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "mongoes_id"
                                }
                            }
                        }
                    }
                }
                remaining_count = self.ext_con['Client'].count(
                    index = self.ext_con['Index'],
                    body = pipeline)
                # ES Stop Point
                return int(remaining_count['count'])
            else:
                pipeline = [{
                        "$match": {
                            "mongoes_id": {
                                "$exists": False
                                }
                            }
                        }, 
                        {
                        "$count": "total_count"
                        }]
                # Mongo Stop Point
                return 0 \
                    if self.ext_con['Collection'].aggregate(pipeline)._has_next() == False \
                    else self.ext_con['Collection'].aggregate(pipeline).next()['total_count']
        except Exception as e:
            print(e)
            ## TBD: Handle Exceptions
            return 0

    def mongoes_id_exists(self):
        try:
            return int(self.ext_con.search(index=self.ext_con['Index'], size=0, body={"query": {"exists" : { "field" : "mongoes_id" }}})['hits']['total'])
        except:
            return 0