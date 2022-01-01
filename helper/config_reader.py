import os.path, json

class ConfigReader():
	def load_config():
		search_paths = ['/etc/mongoes/mongoes.json', './mongoes.json']
		for file_loc in search_paths:
			if os.path.isfile(file_loc) == False:
				pass
			else:
				with open(file_loc) as f:
					# use safe_load instead load
					Config = json.loads(f.read())
					if 'EXTRACT' and 'COMMIT' in Config.keys():
						return Config 	#Returning the handler
					else:
						if 'EXTRACT' in Config.keys() and 'COMMIT' not in Config.keys():
							return 'Commit configuration is unavailable'
						elif 'EXTRACT' not in Config.keys() and 'COMMIT' in Config.keys():
							return 'Extraction configuration is unavailable'
						else:
							return 'Extraction and Commit Configurations are unavailable'
		return "Configuration File not found"

