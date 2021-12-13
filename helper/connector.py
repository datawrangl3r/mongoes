from elasticsearch import Elasticsearch
from pymongo import MongoClient

class Connector:
    def __init__(self, conn_params) -> None:
        self.conn_params = conn_params
        pass

    def elasticsearch_connector(self):
        try:
            database = self.output['EXTRACTION']
            client = Elasticsearch(
                        host = database['HOST'],
                        http_auth = (database['USER'], database['PASSWORD']),
                        scheme = database['PROTOCOL'],
                        port = database['PORT']
            )
            return client
        except Exception as e:
            result_ = {'Success':False, 'Response': 'Extraction Connectivity Failure', 'Message': str(e)}
            return None
    
    def mongodb_connector(self):
        try:
            database = self.output['COMMIT']
            uri = f"mongodb+srv://{username}:{password}@{cluster-address}/{test}?retryWrites=true&w=majority&ssl=true"
            try:
                if database['USER'] != '' and database['USER'] != None:
                    uri = "mongodb://%s:%s@%s:%s" % (database['USER'], database['PASSWORD'], database['HOST'], database['PORT'])
                else:
                    uri = "mongodb://%s:%s" % (database['HOST'], database['PORT'])
            except:
                uri = "mongodb://localhost:27017"
            client = MongoClient(uri)
            cur = client[database['DATABASE']]
            col = cur[database['COLLECTION']]
            return col
        except Exception as e:
            result_ = {'Success':False, 'Response': 'Commit Connectivity Failure', 'Message': str(e)}
            return result_