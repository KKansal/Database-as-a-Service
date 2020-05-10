from pymongo import MongoClient,errors
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
import socket
import pika
import json
import sys

MONGODB_HOST = 'mongodb'
RABBITMQ_HOST = 'rabbitmq'
ZOOKEEPER_HOST = 'zookeeper'
count = 0


logging.getLogger("pika").setLevel(logging.WARNING)

logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.DEBUG, datefmt='%I:%M:%S %p')

logging.getLogger().setLevel(logging.INFO)
client = MongoClient("mongodb://"+ MONGODB_HOST+ ":27017")


db = client.rideshare

db.rides_counter_table.delete_many({})
db.rides_counter_table.insert_one({"http_counter":0})
db.users_counter_table.delete_many({})
db.users_counter_table.insert_one({"http_counter":0})


db.users.create_index('username',unique=True)
db.rides.create_index([('rideId',1)],unique=True)

zk = KazooClient(hosts=ZOOKEEPER_HOST+':2181')

def my_listener(state):
    if state == KazooState.LOST:
        logging.warning("ZooKeeper connection Lost")
    elif state == KazooState.SUSPENDED:
        #Handle being disconnected from Zookeeper
        logging.warning("ZooKeeper connection Suspended")
    else:
        #Handle being connected/reconnected to Zookeeper
        logging.info("ZooKeeper Connected")

zk.add_listener(my_listener)

def syncfunction(channel,method,props,body):
	global count
	count +=1
	logging.info(" Recieved Sync %d request\n Data - %s",count,body)
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

def killserver(queue_name):
	connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
	channel = connection.channel()
	channel.basic_publish(exchange = 'kill_exchange',
							routing_key=queue_name,
							body="")
	connection.close()

def get_previous_data():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
	channel = connection.channel()
	channel.basic_consume(queue='eatQ', on_message_callback=syncfunction)
	logging.info("SYNC Process Started")
	connection.process_data_events()
	logging.info("SYNC Process Completed Slave synced with %d",count)
	connection.close()


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

	search_res = db[collection_name].find(document)
	
	logging.info("Search Results: %d",search_res.count())
	if(search_res.count() == 0):
		logging.info("No Results Matched")
		return 0
	try:
		db[collection_name].delete_many(document)
		logging.info("Operation Successfull")
		return 1
	except:
		return 0


get_previous_data()

class serverSlave():
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,socket_timeout=10))

		self.channel = self.connection.channel()

		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')
		self.channel.exchange_declare(exchange='kill_exchange',exchange_type='direct')
		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')


		self.channel.queue_declare(queue='readQ')
		self.channel.queue_declare(queue="responseQ")
		syncQ = self.channel.queue_declare(queue='', exclusive=True)
		self.syncQ = syncQ.method.queue
		killQ = self.channel.queue_declare(queue='', exclusive=True)
		self.killQ = killQ.method.queue

		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')
		self.channel.queue_bind(exchange="readWrite",queue="responseQ")
		self.channel.queue_bind(exchange="sync",queue=self.syncQ)

		self.channel.basic_consume(queue='readQ', on_message_callback=self.read_db)
		self.channel.basic_consume(queue=self.syncQ, on_message_callback=syncfunction,auto_ack=True)
		self.channel.basic_consume(queue=self.killQ, on_message_callback=self.killfunction,auto_ack = True)
		
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
		
	def start_consuming(self):
		logging.info("Slave Waiting for 'read' messages")
		self.channel.start_consuming()
	
	def killfunction(self,channel,method,props,body):
		self.channel.stop_consuming()
		self.connection.close()



class serverMaster():

	def __init__(self):
		
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,socket_timeout=10))

		self.channel = self.connection.channel()

		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')

		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')
	
		self.channel.queue_declare(queue = "writeQ")
		self.channel.queue_declare(queue = "writeResponseQ")
		self.channel.queue_declare(queue='eatQ',durable= True)

		self.channel.queue_bind(exchange = 'readWrite', queue='writeQ',routing_key='write')
		self.channel.queue_bind(exchange = 'readWrite' , queue="writeResponseQ")

		self.channel.basic_consume(queue='writeQ', on_message_callback=self.write_db)

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
		self.channel.basic_publish(exchange="sync",
								routing_key='syncMessage',
								body=json_data,
								properties=pika.BasicProperties(
                         delivery_mode = 2, 
                      ))

	def start_consuming(self):
		logging.info("Master Waiting for 'write' messages")
		self.channel.start_consuming()	


server = None

zk.start()

container_shortid = socket.gethostname()[0:10]

pid = int(zk.get('/Container_pid/' + container_shortid)[0])
logging.info("PID Of current Container is %d",pid)

node = zk.create("/Election/Slaves/"+ container_shortid,str(pid).encode('utf-8'),ephemeral=True)
node_name = node.split("/")[3]
logging.info("Emphemeral Node created %s",node_name)

if zk.exists("/Election/Master") == None:
	logging.info("Master Does Not exist")
	zk.create("/Election/Master", str(pid).encode('utf-8'),ephemeral=True)
	server_type  = 'master'
else:
	logging.info("Master Exist")
	server_type = 'slave'
	

@zk.DataWatch("/Election/Master")
def watch_parent_node(data,stat, event):
	global pid
	global server
	if(event !=None):
		print("Master Down")
		alive_slaves = zk.get_children("/Election/Slaves")
		master_pid = float('inf')
		for container_name in alive_slaves:
			container_pid = int(zk.get("/Election/Slaves/"+container_name)[0].decode('utf-8'))
			
			if(container_pid < master_pid):
				master_pid = container_pid
		
		logging.info("New Master - %d",master_pid)
		logging.info("pid - %d",pid)
		
		if(master_pid == pid):
			logging.info("Switching to Master Role")
			zk.create("/Election/Master", str(pid).encode('utf-8'),ephemeral=True)
			killserver(server.killQ)
			server = serverMaster()
			server.start_consuming()
			return False
	return True


if(server_type=='master'):
	server = serverMaster()
	server.start_consuming()
else:
	server = serverSlave()
	server.start_consuming()



