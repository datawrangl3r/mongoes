[![N|Solid](https://github.com/datawrangl3r/mongoes/blob/master/mongoes/mongoes.png)](https://github.com/datawrangl3r/mongoes)
# MongoES
A robust elasticsearch to Mongo Migrator. As of now, the data migration is a one-way lane from Elasticsearch to MongoDB. The migration from mongoDB to Elasticsearch is currently underway.

# Features!
  - No external queues/message brokers needed
  - Resumes from the custom **mongoes_id** custom built-in elastic_search checkpoint, in case of data transfer failures.

# Prerequisites:
  - Python - v3.0
  - [Elasticsearch](https://elasticsearch-py.readthedocs.io/en/master/)
  - [pymongo](https://api.mongodb.com/python/current/)
  - asteval

# Steps:
1) Install all the Prerequisites.
2) Clone the repository.
3) Edit the ***mongoes.json*** file according to your requirements.
```json
{
	"EXTRACTION":
		{
			"HOST": "localhost",
			"INDEX": "lorem_ipsum",
			"DBENGINE": "elasticsearch",
			"PORT":9200
		},
	"COMMIT":
		{
			"HOST": "localhost",
			"DATABASE": "plasmodium_proteinbase",
			"COLLECTION": "mongoes",
			"DBENGINE": "mongo",
			"PORT":5432
		}
}
```
4) Make sure that both the elasticsearch and mongoDB services are up and running.
5) And finally, fire the migrator engine by keying in:
```sh
$ python3 __init__.py
```
6) Sit back and relax; for we got you covered! The migration's default value is 1000 entries per transfer.

Happy Wrangling!!! :)

[![N|Solid](http://3.bp.blogspot.com/-UT3SH3wDuYE/WULNcoEkC4I/AAAAAAAAHcc/KaGogqAIvuwPu9UP06jiEl2U39Wj7VX2ACK4BGAYYCw/w800/Logomakr_7nINLC.png)](http://www.datawrangler.in/)