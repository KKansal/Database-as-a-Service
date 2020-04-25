from pymongo import MongoClient,errors
import logging
import pika
import json
import sys

MONGODB_HOST = 'mongodb'
RABBITMQ_HOST = 'rabbitmq'

logging.getLogger("pika").setLevel(logging.WARNING)

logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.DEBUG, datefmt='%I:%M:%S %p')

client = MongoClient("mongodb://"+ MONGODB_HOST+ ":27017")

db = client.rideshare
db.users.create_index('username',unique=True)
db.rides.create_index([('rideId',1)],unique=True)


#db.rides_counter_table.delete_many({})
#db.rides_counter_table.insert_one({"http_counter":0})




def update_data(collection_name,document,filter_data):

	logging.info("Updating based on %s for ONE row satisfying %s",str(document),str(filter_data))
	try:
		db[collection_name].update(filter_data,document)
		return 1
	except:
		logging.warning("Unable to update data to %s based on %s",str(document),str(filter_data))
		return 0


def insert_data(collection_name,document):
	logging.info("Inserting %s into  collection  %s",str(document),str(collection_name))
	try:
		db[collection_name].insert_one(document)
		return 1
	except errors.DuplicateKeyError:
		logging.warning("DuplicateKeyError")
		return 0

def delete_data(collection_name,document):
	logging.info("Deleting %s from Collection %s",str(document),str(collection_name))
	logging.info("Searching %s from Collection %s",str(document),str(collection_name))

	search_res = db[collection_name].find_one(document)
	
	logging.info("Search Results: %s",str(search_res))
	if(search_res==None):
		logging.info("No Results Matched")
		return 0
	try:
		db[collection_name].delete_one(document)
		logging.info("Operation Successfull")
		return 1
	except:
		return 0



"""
9. Read from DB
POST Request
Body Format
{
	"table":"tablename",
	"conditions":{column:value , ...}
}

"""





class serverSlave():
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))

		self.channel = self.connection.channel()

		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')
		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')


		self.channel.queue_declare(queue='readQ')
		self.channel.queue_declare(queue="responseQ")
		self.syncQ = self.channel.queue_declare(queue='', exclusive=True)

		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')
		self.channel.queue_bind(exchange="readWrite",queue="responseQ")
		self.channel.queue_bind(exchange="sync",queue=self.syncQ.method.queue)

		self.channel.basic_consume(queue='readQ', on_message_callback=self.read_db)
		self.channel.basic_consume(queue=self.syncQ.method.queue, on_message_callback=self.syncfunction)

		
		logging.info("Slave Waiting for 'read' messages")
		self.channel.start_consuming()

	def read_db(self,channel,method,props,body):
		logging.info(" [x] Received Read Request for\n Data -%s" % body)
		data_request = json.loads(body)
		response = {"status_code":None,"data":{}}
		try:
			collection = data_request['table']
			condition = data_request['conditions']
		except KeyError:
			logging.warning("KeyError-Not all fields are present")
			response["status_code"] = 400
			return self.sendResponse(json.dumps(response),method,props)

		
		cursor = db[collection].find(condition)
		
		if((cursor.count())>1):
			logging.info("The Query matches %d documents",cursor.count())
		
		elif(cursor.count()==0):
			logging.warning("0 results matched")
			response["status_code"] = 204
			return self.sendResponse(json.dumps(response),method,props)
			
		res = list()
		
		for row in cursor:
			row.pop("_id")
			res.append(row)
		
		logging.info("Results: %s",str(res))

		response["status_code"] = 200
		#TODOCHECK BELOW LINE
		response["data"] = json.dumps(res)
		return self.sendResponse(json.dumps(response),method,props)

	def sendResponse(self,response,method,props):
			self.channel.basic_publish(exchange='readWrite',routing_key=props.reply_to,
                     					properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     					body=str(response))
			self.channel.basic_ack(delivery_tag=method.delivery_tag)
			logging.info("Read Request Acknowledged")	

	def syncfunction(self,channel,method,props,body):
		logging.info(" Recieved Sync request\n Data - %r" % body)
		json_data = json.loads(body)
		try:
			collection_name = json_data['table']
			document =  json_data['data']
			if(json_data['operation']=="update"):
				filter_data = json_data['filter']
		except:
			logging.error("Unable to Sync")

		if(json_data['operation']=="insert"):
			success_code = insert_data(collection_name,document)
		elif(json_data['operation']=="delete"):
			success_code = delete_data(collection_name,document)
		elif(json_data['operation']=="update"):
			success_code = update_data(collection_name,document,filter_data)
		else:
			logging.error("Unable to Sync")
		if(success_code==1):
			logging.info("Sync Operation SuccessFull")
		else:
			logging.error("Unable to Sync")


class serverMaster():

	def __init__(self):
		
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))

		self.channel = self.connection.channel()

		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')

		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')
	
		self.channel.queue_declare(queue = "writeQ")
		self.channel.queue_declare(queue = "writeResponseQ")

		self.channel.queue_bind(exchange = 'readWrite', queue='writeQ',routing_key='write')
		self.channel.queue_bind(exchange = 'readWrite' , queue="writeResponseQ")

		self.channel.basic_consume(queue='writeQ', on_message_callback=self.write_db)

		logging.info("Master Waiting for 'write' messages")
		self.channel.start_consuming()

	def write_db(self,channel,method,props,body):
		logging.info("Received Write request\n Data - %r" % body)
		json_data = json.loads(body)
		response = {"status_code":None}
		try:
			collection_name = json_data['table']
			document =  json_data['data']
			if(json_data['operation']=="update"):
				filter_data = json_data['filter']
		except KeyError:
			response["status_code"] = 400
			logging.warning("Bad Request Recieved not all Fields are present in the Request")
			return self.sendResponse(json.dumps(response),method,props)

		if(json_data['operation']=="insert"):
			success_code = insert_data(collection_name,document)
		elif(json_data['operation']=="delete"):
			success_code = delete_data(collection_name,document)
		elif(json_data['operation']=="update"):
			success_code = update_data(collection_name,document,filter_data)
		else:
			response["status_code"] = 400
			logging.warning("Bad Request - Operation '%s' not supported",json_data['operation'])
			return self.sendResponse(json.dumps(response),method,props)
		if(success_code==1):
			response["status_code"] = 200
			self.sendSyncMessage(body)
			logging.info("Write Operation Successfull")
			return self.sendResponse(json.dumps(response),method,props)
		else:
			response["status_code"] = 405
			logging.warning("Error Encountered while Performing DB operations - Method Not Allowed")
			return self.sendResponse(json.dumps(response),method,props)

	def sendResponse(self,response,method,props):
			self.channel.basic_publish(exchange='readWrite',routing_key=props.reply_to,
                     					properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     					body=str(response))
			self.channel.basic_ack(delivery_tag=method.delivery_tag)
			logging.info("Write Request Acknowleged")

	def sendSyncMessage(self,json_data):
		logging.info("Publishing Sync Messages")
		self.channel.basic_publish(exchange="sync",routing_key='syncMessage',body=json_data)	






if(sys.argv[1] == "1"):
	server = serverMaster()
else:
	server = serverSlave()






