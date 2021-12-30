import copy
from pymongo import DESCENDING
import elasticsearch.helpers

class Loader:
    def __init__(self, ext_con, com_con):
        # 'Client': client, 
        # 'Cursor': None,
        # 'Collection': None,
        # 'Index': params['INDEX'],
        # 'Message': 'Connected', 
        # 'Trace': None
        self.ext_con = ext_con
        self.com_con = com_con
        self.resume_point = None

    # def this_is_just_a_drill(self):
    #     try:
    #         body_ = {"query": {"exists": {"field": "mongoes_id"}}}
    #         page_ = self.ext_con.search(
    #                     index = self.ext_con['Index'], 
    #                     size = 1000, 
    #                     body=body_)
    #         hits_ = page_['hits']['hits']
    #         self.no_trace(hits_)
    #         return {"status": "succeeded"}
    #     except Exception as e:
    #         print (e)
    #         hits_ = []
    #     return hits_

    def read_data(self):
        try:
            self.resume_point = self.find_resume_point()
            if self.ext_con['Index'] != None:
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
                            size = 10000, 
                            body=body_
                        )
                hits_ = page_['hits']['hits']
                return hits_
                # return {"status": "succeeded"}
            else:
                # TBD: ES as destination
                do_ntn = 0
        except Exception as e:
            # TBD: Handle Exception
            hits_ = []
        return hits_

    def tag_documents(self, hits_):
        sour_trace = []
        dest_trace = []

        if self.ext_con['Index'] != None:
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
            #TBD Handle Mongodb as source
            do_ntn = 0
        return sour_trace, dest_trace

    def write_data(self, list_):
        if list_ == []:
            return []
        try:
            if self.ext_con['Index'] != None:
                ext_trace, com_trace = self.tag_documents(list_)
                elasticsearch.helpers.bulk(self.ext_con['Client'], ext_trace)
                if com_trace != []:
                    self.com_con['Collection'].insert_many(com_trace, ordered=False)
                # TBD: Handle successful write
            else:
                # TBD: ES as destination
                do_ntn = 0
        except Exception as e:
            return {}

    # def no_trace(self, list_):
    #     try:
    #         mongo_copy = []
    #         for x in list_:
    #             mongo_copy.append(x['_source'])
    #             mongo_copy[-1]['_id'] = mongo_copy[-1]['mongoes_id']
    #             mongo_copy[-1].pop('mongoes_id')
    #         if mongo_copy != []:
    #             self.com_con.insert_many(mongo_copy, ordered=False)
    #             return {"success":"superman"}
    #     except Exception as e:
    #         print (e)
    #         return {}

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
            if self.ext_con['Index'] != None:
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

    # def find_index_size_es(self):    #to_be_migrated
    #     try:
    #         return int(self.ext_con.search(index=self.ext_con['Index'], size=0)['hits']['total'])
    #     except:
    #         return 0

    def find_remaining_count(self):
        try:
            if self.ext_con['Index'] != None:
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
                pipeline = [
                    {
                        "$group": {
                            "_id": {
                                "$ifNull": ["$mongoes_id", False]
                            }, 
                            "count": {
                                "$sum": 1
                                }
                            }
                        }
                    ]
                # Mongo Stop Point
                return int(self.ext_con['Cursor'].aggregate(pipeline))
        except Exception as e:
            ## TBD: Handle Exceptions
            return 0

    def mongoes_id_exists(self):
        try:
            return int(self.ext_con.search(index=self.ext_con['Index'], size=0, body={"query": {"exists" : { "field" : "mongoes_id" }}})['hits']['total'])
        except:
            return 0