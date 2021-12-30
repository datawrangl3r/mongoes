import copy
from pymongo import DESCENDING
import elasticsearch

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

    # def this_is_just_a_drill(self):
    #     try:
    #         body_ = {"query": {"exists": {"field": "mongoes_id"}}}
    #         page_ = self.ext_con.search(
    #                     index = self.indx_name, 
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
                page_ = self.ext_con.search(
                            index = self.indx_name, 
                            size = 1000, 
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

    def tag_the_document(self):
        self.mark+=1
        return self.mark

    def write_data(self, list_):
        if list_ == []:
            return []
        try:
            if self.ext_con['Index'] != None:
                trace_ = []
                mongo_copy = []
                for x in list_:
                    trace_copy = copy.deepcopy(x['_source'])
                    trace_.append(trace_copy)
                    mongo_copy.append(x['_source'])
                    _id = self.tag_the_document()
                    trace_[-1]['mongoes_id'] = _id
                    mongo_copy[-1]['_id'] = _id
                    try:
                        mongo_copy[-1].pop('mongoes_id')
                    except Exception as e:
                        # TBD: Handle Exception
                        print(e)
                es_register_ = [{"_op_type": "update", "_index": list_[x]["_index"], "_type": list_[x]["_type"], "_id": list_[x]["_id"], 'doc': {"mongoes_id": trace_[x]["mongoes_id"]}} for x in range(len(trace_))]
                elasticsearch.helpers.bulk(self.ext_con, es_register_)
                if mongo_copy != []:
                    self.com_con.insert_many(mongo_copy, ordered=False)
                    print ('copied')
                return trace_
                # TBD: Handle successful write
            else:
                # TBD: ES as destination
                do_ntn = 0
        except Exception as e:
            return {}

    def no_trace(self, list_):
        try:
            mongo_copy = []
            for x in list_:
                mongo_copy.append(x['_source'])
                mongo_copy[-1]['_id'] = mongo_copy[-1]['mongoes_id']
                mongo_copy[-1].pop('mongoes_id')
            if mongo_copy != []:
                self.com_con.insert_many(mongo_copy, ordered=False)
                return {"success":"superman"}
        except Exception as e:
            print (e)
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
                res_point = self.ext_con.search(
                                    index = self.indx_name, 
                                    size = 0, 
                                    body = pipeline)
                return int(res_point['aggregations']['max_mongoes_id']['value'])
            else:
                # Mongo's Resume Point
                return int(self.com_con.find().sort("_id", DESCENDING).limit(1)[0]["_id"])
        except Exception as e:
            return 0

    # def find_index_size_es(self):    #to_be_migrated
    #     try:
    #         return int(self.ext_con.search(index=self.indx_name, size=0)['hits']['total'])
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
                # ES Stop Point
                return int(
                    self.ext_con['Client'].search(
                    index = self.indx_name, 
                    size = 0, 
                    body = pipeline)['hits']['total']
                )
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
            print (e)
            return 0

    def mongoes_id_exists(self):
        try:
            return int(self.ext_con.search(index=self.indx_name, size=0, body={"query": {"exists" : { "field" : "mongoes_id" }}})['hits']['total'])
        except:
            return 0