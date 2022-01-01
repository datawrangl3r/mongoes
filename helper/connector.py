from elasticsearch import Elasticsearch
from pymongo import MongoClient

class Connector:
    def __init__(self, conn_params) -> None:
        self.conn_params = conn_params
        pass

    def establish_connection(self, conn_nature) -> dict:
        return self.elasticsearch_connector(conn_nature) if \
            self.conn_params[conn_nature]['DBENGINE'].lower() == 'elasticsearch' \
            else self.mongodb_connector(conn_nature)

    def elasticsearch_connector(self, conn_nature) -> dict:
        try:
            params = self.conn_params[conn_nature]
            client = Elasticsearch(
                        host = params['HOST'],
                        http_auth = (params['USER'], params['PASSWORD']),
                        scheme = params['PROTOCOL'],
                        port = params['PORT']
            )
            return {
                'Client': client, 
                'Cursor': None,
                'Collection': None,
                'Index': params['INDEX'],
                'Message': 'Connected', 
                'Trace': None
            }
        except Exception as e:
            return {
                'Client': None,
                'Cursor': None,
                'Collection': None,
                'Message': 'Failed', 
                'Trace': str(e)
            }
    
    def mongodb_connector(self, conn_nature) -> dict:
        try:
            params = self.conn_params[conn_nature]
            ssl_flag = "&ssl=true" if params['SSL'] == True else ""
            creds = f"{params['USER']}:{params['PASSWORD']}@" if params['USER'] != "" else ""
            try:
                uri = f"mongodb+srv://{creds}{params['HOST']}"+\
                        f"/{params['DATABASE']}?retryWrites=true&w=majority{ssl_flag}"
                client = MongoClient(uri)
            except:
                uri = f"mongodb://{creds}{params['HOST']}:{params['PORT']}/{params['DATABASE']}?authSource=admin"
                client = MongoClient(uri)
            cur = client[params['DATABASE']]
            col = cur[params['COLLECTION']]
            return {
                'Client': client, 
                'Cursor': cur,
                'Collection': col,
                'Message': 'Connected', 
                'Trace': None
            }
        except Exception as e:
            return {
                'Client': None,
                'Cursor': None,
                'Collection': None,
                'Index': None,
                'Message': 'Failed', 
                'Trace': str(e)
            }