import os.path, json

class config_reader():
	def load_config():
		search_paths = ['/etc/mongoes/mongoes.json', './mongoes.json']
		for file_loc in search_paths:
			if os.path.isfile(file_loc) == False:
				pass
			else:
				with open(file_loc) as f:
					# use safe_load instead load
					Config = json.loads(f.read())
					if 'EXTRACTION' and 'COMMIT' in Config.keys():
						return Config 	#Returning the handler
					else:
						if 'EXTRACTION' in Config.keys() and 'COMMIT' not in Config.keys():
							return 'Commit configuration is unavailable'
						elif 'EXTRACTION' not in Config.keys() and 'COMMIT' in Config.keys():
							return 'Extraction configuration is unavailable'
						else:
							return 'Extraction and Commit Configurations are unavailable'
		return "Configuration File not found"

