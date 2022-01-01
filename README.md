[![MongoES](https://github.com/datawrangl3r/mongoes/blob/master/mongoes.png)](https://github.com/datawrangl3r/mongoes)
# MongoES

> version 2.0

A two-way Migrator (supports Elasticsearch --> MongoDB & MongoDB --> Elasticsearch)

## Features!

- Migration supports Elasticsearch --> MongoDB & MongoDB --> Elasticsearch
- No external queues/message brokers needed
- Resumes from the custom `mongoes_id` custom built-in checkpoint, in case of data transfer failures.

## Prerequisites:

  - Python - v3.8
  - [Elasticsearch](https://elasticsearch-py.readthedocs.io/en/master/)
  - [pymongo](https://api.mongodb.com/python/current/)

## Steps:

1) Clone the repository.

2) Initialize virtual environment and install all the Prerequisites.

```Bash
$ python3 -m venv .env
$ source .env/bin/activate
$ pip install -r requirements.txt
```

3) Edit the ***mongoes.json*** file according to your requirements.

*If Elasticsearch is your source:*

```json
{
	"COMMIT": {
		"HOST": "localhost",
		"INDEX": "shakespeare4",
		"USER": "elastic",
		"PASSWORD": "passopasso",
		"DBENGINE": "elasticsearch",
		"PORT":9200,
		"PROTOCOL": "http"
	},
	"EXTRACT": {
		"HOST": "localhost",
		"DATABASE": "shakespeare",
		"COLLECTION": "shakespeare",
		"USER": "mongobongo",
		"PASSWORD": "passopasso",
		"DBENGINE": "mongo",
		"PORT":27017,
		"SSL": false
	},
	"SETTINGS": {
		"FREQUENCY": 10000
	}
}
```

*or in case of mongodb is the source:*

```json
{
	"COMMIT": {
		"HOST": "localhost",
		"INDEX": "shakespeare4",
		"USER": "elastic",
		"PASSWORD": "passopasso",
		"DBENGINE": "elasticsearch",
		"PORT":9200,
		"PROTOCOL": "http"
	},
	"EXTRACT": {
		"HOST": "localhost",
		"DATABASE": "shakespeare",
		"COLLECTION": "shakespeare",
		"USER": "mongobongo",
		"PASSWORD": "passopasso",
		"DBENGINE": "mongo",
		"PORT":27017,
		"SSL": false
	},
	"SETTINGS": {
		"FREQUENCY": 10000
	}
}
```

4) Make sure that both the elasticsearch and mongoDB services are up and running.

5) And finally, fire the migrator by keying in:

```sh
$ python3 __init__.py
```
6) Sit back and relax; for we got you covered! The migration's default value is 1000 entries per transfer.

Happy Wrangling!!! :)

[![Datawrangler](http://3.bp.blogspot.com/-UT3SH3wDuYE/WULNcoEkC4I/AAAAAAAAHcc/KaGogqAIvuwPu9UP06jiEl2U39Wj7VX2ACK4BGAYYCw/w800/Logomakr_7nINLC.png)](http://www.datawrangler.in/)