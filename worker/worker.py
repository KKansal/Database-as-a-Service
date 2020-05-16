from pymongo import MongoClient,errors
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
import socket
import pika
import json
import sys

"""
Initializing Global Variables

"""
#Initializing Hostnames for all the services Required these available through orch_net network the
#worker container is attached to by the orchestrator
MONGODB_HOST = 'mongodb'
RABBITMQ_HOST = 'rabbitmq'
ZOOKEEPER_HOST = 'zookeeper'

#Global counter for Determining Message Number in the Sync Operation 
count = 0

#Server variable to assume the role as Master or worker. 
server = None



# def my_listener(state):
#     if state == KazooState.LOST:
#         logging.warning("ZooKeeper connection Lost")
#     elif state == KazooState.SUSPENDED:
#         #Handle being disconnected from Zookeeper
#         logging.warning("ZooKeeper connection Suspended")
#     else:
#         #Handle being connected/reconnected to Zookeeper
#         logging.info("ZooKeeper Connected")

# zk.add_listener(my_listener)

"""
This Function is triggered in two cases when a  master sends a write message into the Slaves syncQ 
or at the startup of a new worker.The function processes the write Request and calls the appropriate
function to perform updates on the database.
Note - The write message is bound to be successfull since the message was successfully processed
by the master and only then published to the syncQ

"""
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

"""
This Function is called whenever a Slave is elected as a Master. The Slave in order to become a
master has to stop consuming messages from the readQ, SyncQ etc. However the Slave gets notified 
about the new master in a different thread where the Zookeeper watch is triggered , Since Pika is 
not thread safe we cannot end the connection from this thread, hence we call this function which 
sends a message to the killQ which the other thread along with readQ and SyncQ is consuming messages
from and thread itself closes the connection.  
"""

def killserver(queue_name):
	connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
	channel = connection.channel()
	channel.basic_publish(exchange = 'kill_exchange',
							routing_key=queue_name,
							body="")
	connection.close()

"""
This function is called whenever a new worker is started up. It is responsible for updating the
database of the Worker to a the present consistent state.It reads all the previous write messages 
from the eatQ and performs all the write operations using the syncfunction provided. 
"""
def get_previous_data():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
	channel = connection.channel()
	channel.basic_consume(queue='eatQ', on_message_callback=syncfunction)
	logging.info("SYNC Process Started")
	connection.process_data_events()
	logging.info("SYNC Process Completed Slave synced with %d",count)
	connection.close()

"""
The below functions are called by the write_db and syncfunction to perform the actual database
operation required 

"""

""" This function performs update operation on the database """
def update_data(collection_name,document,filter_data):

	logging.info("Updating based on %s for ONE row satisfying %s",str(document),str(filter_data))
	try:
		db[collection_name].update(filter_data,document)
		return 1
	except:
		logging.warning("Unable to update data to %s based on %s",str(document),str(filter_data))
		return 0

""" This function performs insert operation on the database """
def insert_data(collection_name,document):
	logging.info("Inserting %s into  collection  %s",str(document),str(collection_name))
	try:
		db[collection_name].insert_one(document)
		return 1
	except errors.DuplicateKeyError:
		logging.warning("DuplicateKeyError")
		return 0

""" This function performs delete operation on the database """
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




class serverSlave():

	"""
	This class contains all the code for if a worker assumes its role as a Slave
	A Slave is supposed to serve read Requests from the orchestrator.
	The slave consumes messages from the readQ ( readWrite exchange) , syncQ (sync Exchange ) and the killQ ( kill exchange).
	The slave publishes messages to the responseQ ( readWrite Exchange)

	"""
	def __init__(self):
		#Initiate connection to the RabbitMQ server
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,socket_timeout=10))

		#Estabilish a channel with the RabbitMQ server
		self.channel = self.connection.channel()

		#Declare all the exchanges required by the slave.
		#Note -An exchange/queue is created if and only if it does not exist before.
		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')
		self.channel.exchange_declare(exchange='kill_exchange',exchange_type='direct')
		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')

		#Declare readQ,responseQ,syncQ(exclusive to Slave) and killQ(exclusive to Slave).
		self.channel.queue_declare(queue='readQ')
		self.channel.queue_declare(queue="responseQ")
		syncQ = self.channel.queue_declare(queue='', exclusive=True)
		self.syncQ = syncQ.method.queue
		killQ = self.channel.queue_declare(queue='', exclusive=True)
		self.killQ = killQ.method.queue

		#Bind the Queues to their respect exchanges and the routing key.
		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')
		self.channel.queue_bind(exchange="readWrite",queue="responseQ")
		self.channel.queue_bind(exchange="sync",queue=self.syncQ)

		#Specify actions on encoutering any messages in each of the Queues.
		self.channel.basic_consume(queue='readQ', on_message_callback=self.read_db)
		self.channel.basic_consume(queue=self.syncQ, on_message_callback=syncfunction,auto_ack=True)
		self.channel.basic_consume(queue=self.killQ, on_message_callback=self.killfunction,auto_ack = True)

	"""
	This function is triggered when a read request is sent by the orchestrator on the readQ
	The function processes the read Request and performs the required read operation.
	It calls the sendResponse function with the data and the status code in the form of a JSON 
	object as {"status_code":,"data":}.
	"""	
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

	"""
	This function sends a response back to the orchestrator the routing key and the correlation id
	are fetched from the original message the response if for . this routing key and correlation id 
	are assumed to be set by the original sender of the message.
	"""
	def sendResponse(self,response,method,props):
			self.channel.basic_publish(exchange='readWrite',routing_key=props.reply_to,
                     					properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     					body=str(response))
			self.channel.basic_ack(delivery_tag=method.delivery_tag)
			logging.info("Read Request Acknowledged")	

	"""
	This function starts the process of consuming messages from the specified Queues and on encoutering a 
	message triggers the function it specified.
	"""
	def start_consuming(self):
		logging.info("Slave Waiting for 'read' messages")
		self.channel.start_consuming()

	"""	
	This function stops the process of reading any messages and closes the connection with the RabbitMQ server.
	"""
	def killfunction(self,channel,method,props,body):
		self.channel.stop_consuming()
		self.connection.close()



class serverMaster():
	"""
	This class contains all the code for if a worker assumes its role as a Master
	A Master is supposed to serve write Requests from the orchestrator.
	The Master consumes messages from the writeQ ( readWrite exchange) only.
	The slave publishes messages to the writeResponseQ ( readWrite Exchange ) syncQ ( sync exchange) and the eatQ ( sync exchange)

	"""
	def __init__(self):
		
		#Estailish a connection to the RabbitMQ server
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,socket_timeout=10))

		#Create a Channel 
		self.channel = self.connection.channel()

		#Declare the required exchanges
		#Note - An exchange/queue is created if and only if it does not exist before.
		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')
		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')
	
		#Declare the required Queues.
		self.channel.queue_declare(queue = "writeQ")
		self.channel.queue_declare(queue = "writeResponseQ")
		self.channel.queue_declare(queue='eatQ',durable= True)

		#Bind the queues to their respective exchanges and routing key
		self.channel.queue_bind(exchange = 'readWrite', queue='writeQ',routing_key='write')
		self.channel.queue_bind(exchange = 'readWrite' , queue="writeResponseQ")

		self.channel.basic_consume(queue='writeQ', on_message_callback=self.write_db)

	"""
	This function is called when the master recieves a write message from the orchestrator on the writeQ.
	This function processes the write required and calls the function for the required Database function
	after performing the database operation it sends the data back to the orchestrator(  with sendMessage function)
	in the form a JSON object {"status_code":}.

	"""
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

	"""
	This function sends a response back to the orchestrator ,the routing key and the correlation id
	are fetched from the original message the response if for . this routing key and correlation id 
	are assumed to be set by the original sender of the message.
	"""
	def sendResponse(self,response,method,props):
			self.channel.basic_publish(exchange='readWrite',routing_key=props.reply_to,
                     					properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     					body=str(response))
			self.channel.basic_ack(delivery_tag=method.delivery_tag)
			logging.info("Write Request Acknowleged")

	"""
	Whenever a write message sent by the orchestrator is successfull in updating the database this function
	is called. It sends a sync message to the sync exchange of all the queues with the data as the original write
	request , this ensures that the database of all slaves is updated and is consitent with database of the master.
	"""
	def sendSyncMessage(self,json_data):
		logging.info("Publishing Sync Messages")
		self.channel.basic_publish(exchange="sync",
								routing_key='syncMessage',
								body=json_data,
								properties=pika.BasicProperties(
                         delivery_mode = 2, 
                      ))

	"""
	This function starts the process of consuming messages from the specified Queues and on encoutering a 
	message triggers the function it specified.
	"""
	def start_consuming(self):
		logging.info("Master Waiting for 'write' messages")
		self.channel.start_consuming()	


#Supressing PIKA Logger to warnings
logging.getLogger("pika").setLevel(logging.WARNING)

logging.basicConfig(format='%(asctime)s [%(levelname)s]:%(message)s', level=logging.DEBUG, datefmt='%I:%M:%S %p')

#Setting the Logger to INFO to see All messages
logging.getLogger().setLevel(logging.INFO)


#Connecting to MongoDB database
client = MongoClient("mongodb://"+ MONGODB_HOST+ ":27017")

#The Database Name for the Service is set to rideshare and all data is stored in this database.
db = client.rideshare

#Initializing the counts for the user and rides service to be zero.
db.rides_counter_table.delete_many({})
db.rides_counter_table.insert_one({"http_counter":0})
db.users_counter_table.delete_many({})
db.users_counter_table.insert_one({"http_counter":0})

#These Enforce contraints :-
db.users.create_index('username',unique=True) #Each username in the users table should be unique
db.rides.create_index([('rideId',1)],unique=True) #Each 'rideID' in the rides table should be unique and stored in ascending order.

#Before assuming the role of Master or slave update the database by getting previous updates.
get_previous_data()


#Connecting to the Zookeeper Server.
zk = KazooClient(hosts=ZOOKEEPER_HOST+':2181')
zk.start()

#Get the Container Id where the Worker is running
container_shortid = socket.gethostname()[0:10]

#Get the PID of the container from the '/Container_pid/CONTAINER_ID" which would have been set by the ORchestrator.
pid = int(zk.get('/Container_pid/' + container_shortid)[0])
logging.info("PID Of current Container is %d",pid)

#Create an Emphmeral node for the current worker in the '/Election/Slave' directory.
node = zk.create("/Election/Slaves/"+ container_shortid,str(pid).encode('utf-8'),ephemeral=True)
node_name = node.split("/")[3]
logging.info("Emphemeral Node created %s",node_name)

# If master is not present assume the role of master by creating the node "/Election/Master'
# otherwise assume the role as a Slave
if zk.exists("/Election/Master") == None:
	logging.info("Master Does Not exist")
	zk.create("/Election/Master", str(pid).encode('utf-8'),ephemeral=True)
	server_type  = 'master'
else:
	logging.info("Master Exist")
	server_type = 'slave'
	



"""
The worker sets an DataWatch on '/Election/Master' so that it is notified when the Master Crashes
If a Master Crashes the Leader Election process starts up

Leader Election algorithm:-

1) alive_slaves = Get the PID of all the Alive workers
2) master_pid = Find the lowest PID from the above list
3)if ( Worker_pid  == master_pid) change role to master
  else continue role as the slave.


"""
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



"""
Assume the role of Master or Slave based on if the master is present or not.
"""
if(server_type=='master'):
	server = serverMaster()
	server.start_consuming()
else:
	server = serverSlave()
	server.start_consuming()



