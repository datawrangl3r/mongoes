import copy
from elasticsearch import Elasticsearch
from pymongo import MongoClient, DESCENDING
from asteval import Interpreter
from config_reader import config_reader
from elasticsearch import helpers




aeval = Interpreter()

class Migrator():
	def __init__(self):
		self.output  = config_reader.load_config()
		self.ext_cur = self.ext_connection()
		self.com_cur = self.com_connection()
		self.indx_name = self.output['EXTRACTION']['INDEX']
		self.indx_size_es = self.find_stop_point_es() #To be transferred
		print (self.indx_size_es)
		try:
			self.mongo_mark=self.find_resume_point_mongo()
		except:
			self.mongo_mark=0

		try:
			self.es_mark=self.find_resume_point_es()
		except:
			self.es_mark=0

		try:
			self.stop_mark = self.find_stop_point_es()
		except:
			self.stop_mark = 0

		self.mark = self.es_mark
		
		
		if self.indx_size_es == 0:
			print ('Transfer Completed!!!')
			return None

		while self.stop_mark != 0:
			self.data_transfer()
			self.es_mark=self.find_resume_point_es()
			self.mark = self.es_mark
			self.stop_mark = self.find_stop_point_es()
			print ('%s - Partition transfer completed'%(str(self.mark)))
			print ('%s - documents left'%(str(self.stop_mark)))
		print ('Transfer Completed!!!')
		return None

	def ext_connection(self):
		try:
			database = self.output['EXTRACTION']
			if database['DBENGINE'].lower().find('elastic')!=-1:
				client = Elasticsearch(host = database['HOST'])
			return client
		except Exception as e:
			result_ = {'Success':False, 'Response': 'Extraction Connectivity Failure', 'Message': str(e)}
			return result_

	def com_connection(self):
		try:
			database = self.output['COMMIT']
			client = MongoClient("mongodb+srv://username:password@maxonic.ui8v9.mongodb.net/dbName?ssl=true&ssl_cert_reqs=CERT_NONE&retryWrites=true&w=majority")
			cur = client.test
			col = cur[database['COLLECTION']]
			return col
		except Exception as e:
			result_ = {'Success':False, 'Response': 'Commit Connectivity Failure', 'Message': str(e)}
			return result_

	def this_is_just_a_drill(self):
		try:
			body_ = {"query": {"exists": {"field": "mongoes_id"}}}
			page_ = self.ext_cur.search(index = self.indx_name, size = 1000, body=body_)
			hits_ = page_['hits']['hits']
			self.no_trace(hits_)
			return {"status": "succeeded"}
		except Exception as e:
			hits_ = []
		return hits_

	def data_transfer(self):
		try:
			body_ = {"query": {"bool": {"must_not": {"exists": {"field": "mongoes_id"}}}}}
			page_ = self.ext_cur.search(index = self.indx_name, size = 1000, body=body_)
			hits_ = page_['hits']['hits']
			if hits_ != []:
				self.leave_trace(hits_)
			return {"status": "succeeded"}
		except Exception as e:
			print (e)
			hits_ = []
		return hits_

	def tag_the_document(self):
		self.mark+=1
		return self.mark

	def leave_trace(self, list_):
		if list_ == []:
			return {}
		try:
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
					do_ntn = 0
			es_register_ = [{"_op_type": "update", "_index": list_[x]["_index"], "_type": list_[x]["_type"], "_id": list_[x]["_id"], 'doc': {"mongoes_id": trace_[x]["mongoes_id"]}} for x in range(len(trace_))]
			helpers.bulk(self.ext_cur, es_register_)
			if mongo_copy != []:
				self.com_cur.insert_many(mongo_copy, ordered=False)
				print ('copied')
			return trace_
		except Exception as e:
			print(e)
			return {}

	def no_trace(self, list_):
		try:
			mongo_copy = []
			for x in list_:
				mongo_copy.append(x['_source'])
				mongo_copy[-1]['_id'] = mongo_copy[-1]['mongoes_id']
				mongo_copy[-1].pop('mongoes_id')
			if mongo_copy != []:
				self.com_cur.insert_many(mongo_copy, ordered=False)
				return {"success":"superman"}
		except Exception as e:
			print (e)
			return {}

	def validate_conf(self):
		try:
			cur = self.ext_connection()
		except Exception as e:
			return 'Something is wrong with the connection. Check your connection/config file'

		try:
			cur = self.com_connection()
		except Exception as e:
			return 'Something is wrong with the connection. Check your connection/config file'

		return None
		## Need to include other test validations right here

	def find_resume_point_mongo(self):
		try:
			ans = self.com_cur.find().sort("_id", DESCENDING).limit(1)[0]["_id"]
			return (self.com_cur.find().sort("_id", DESCENDING).limit(1)[0]["_id"])
		except Exception as e:
			return 0

	def find_resume_point_es(self):
		try:
			res_point = self.ext_cur.search(index = self.indx_name, size=0, body={"aggs" : {"max_mongoes_id" : { "max" : { "field" : "mongoes_id" }}}})
			if res_point == None:
				return 0
			else:
				return int(res_point['aggregations']['max_mongoes_id']['value'])
		except Exception as e:
			return 0

	def find_index_size_es(self):	#to_be_migrated
		try:
			ans2 = self.ext_cur.search(index=self.indx_name, size=0)['hits']['total']
			return ans2['value']
		except:
			return 0

	def find_stop_point_es(self):
		try:
			ans3 = self.ext_cur.search(index = self.indx_name, size=0, body={"query": {"bool": {"must_not": {"exists": {"field": "mongoes_id"}}}}})['hits']['total']
			return ans3['value']
		except Exception as e:
			return 0

	def mongoes_id_exists(self):
		try:
			ans4 = self.ext_cur.search(index=self.indx_name, size=0, body={"query": {"exists" : { "field" : "mongoes_id" }}})['hits']['total']
			return ans4['value']
		except:
			return 0

if __name__ == "__main__":
	mig_obj = Migrator()
	# if mig_obj.validate_conf() == None:
		# mig_obj.init_migration()
