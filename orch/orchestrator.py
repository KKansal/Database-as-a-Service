from flask import Flask,request,jsonify,abort
from kazoo.client import KazooClient
from kazoo.client import KazooState
from pymongo import MongoClient
from math import ceil
import threading
import docker
import json
import pika
import uuid
import time
import logging

#Sleep for RabbitMQ to startup and initialize
time.sleep(10)

#Initialize the Hostnames for the services required.
ZOOKEEPER_HOST = "zookeeper"
RABBITMQ_HOST = "rabbitmq"
MONGO_HOST = "orch_data"


# ZOOKEEPER_HOST = "localhost"
# RABBITMQ_HOST = "localhost"
# MONGO_HOST = "localhost"


db = None

"""
This function is to determine if the scaling process has been already started or not.
This function returns the flag which indicates whether the scaling process has been started
or not. If flag value is false this indicates the scaling process has not been started
if the flag value is true this means the scaling process has started
"""
def get_flag():
	return db['counter'].find_one({})['flag']

"""
This function  updates the database to indicate that the scaling process has been started
and sets the flag value to be true.

"""
def update_flag():
	db['counter'].update_one({},{'$set':{'flag':True}})

"""
This function reads the count ( indicating the number request received to the read API)
variable from the database and returns it.

"""
def get_num_requests():
	return db['counter'].find_one({})['count']

"""
This function sets the counter value to equal to zero. in the database.
"""
def reset_http_counter():
	db['counter'].update_one({},{"$set":{"count":0,'flag':True}})

"""
This function increments the count of the number of requests in the database by 1

"""
def inc_counter():
	db['counter'].update_one({},{'$inc':{'count':1}})


"""
This function performs the scale up and scale down operations based on the number of requests recieved.
It calculates number of workers which are supposed to running and accordingly scales up and down by
calling the start_worker and stop_worker functions repeatedly.
"""
def manage_containers(curr_req):

	#Calculate how many workers should be running.
	target_workers = max(ceil(curr_req/20),1)

	#Calculate how many workers are actually running
	# Note- 1 is subtracted because one of the workers will be the Slave worker. 
	workers_running = len(zk.get_children("/Election/Slaves")) - 1

	#Calculate the difference
	diff = target_workers - workers_running

	logging.info("Number of workers to be added %d",diff)

	#If different is positive this means the scale up process has to be started and the start_worker fucntion is called.
	while(diff > 0):
		start_worker()
		diff -=1

	#If different is negative this means that scale down operation has to be performed the stop_worker function has to be called.
	while(diff < 0):
		stop_worker()
		diff +=1

"""
Note - This function works on a different thread.
This intitiates the Scaling process for after 2 minute intervals.
It blocks for 2 minute intervals and fetches the number of requests 
and calls the manage_containers function to perform scaling.
"""
def scale():
	while(True):
	   print("Reading Requests for 2 mins")
	   time.sleep(120)
	   logging.info("Scale Process started")
	   num_requests = get_num_requests()
	   reset_http_counter()

	   manage_containers(num_requests)


#Client for docker.
client = docker.from_env()


class rabbitmqClient():
	""" 
	This class contains the code for communicating with the other workers via RabbitMQ 
	"""
	def __init__(self):
		# Set up Connection
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST,heartbeat=0))
		self.channel = self.connection.channel()

		# Declare Exchange
		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')

		# Declare Sync Exchange
		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')

		# Declare Eat-UP Queue
		self.channel.queue_declare(queue="eatQ",durable=True)

		# Declare readQ
		self.channel.queue_declare(queue='readQ')

		# Declare writeQ
		self.channel.queue_declare(queue="writeQ")

		# Declare responseQ
		responseQ = self.channel.queue_declare(queue="responseQ")
		self.responseQ = responseQ.method.queue

		# Declare writeResponseQ 
		writeResponseQ = self.channel.queue_declare(queue="writeResponseQ")
		self.writeResponseQ = writeResponseQ.method.queue


		# Bind the respective Queues with their respective exchanges and routing keys.
		self.channel.queue_bind(exchange='readWrite', queue='writeQ',routing_key='write')

		self.channel.queue_bind(exchange="readWrite", queue=self.responseQ)

		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')

		self.channel.queue_bind(exchange = 'readWrite' , queue = self.writeResponseQ)

		self.channel.queue_bind(exchange="sync",queue='eatQ')

		# Specify the Queues from which the messages are supposed to be consumed and the function
		# which are supposed to be called on encountering a message.
		self.channel.basic_consume(
			queue=self.responseQ,
			on_message_callback=self.on_response)

		self.channel.basic_consume(
			queue=self.writeResponseQ,
			on_message_callback=self.on_response)
		
		# self.channel.start_consuming()
		
	"""
	This function is used to publish a message to the readWrite exchange .
	The routing key decides which queue the message is supposed to be sent to.
	This function after sending a message blocks until a reply is recieved
	and finally returns the reply.
	"""
	def sendMessage(self,routing_key,message,callback_queue):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange='readWrite',
							properties=pika.BasicProperties(
								reply_to = callback_queue,
								correlation_id=self.corr_id,
							),
							routing_key=routing_key,
							body=message)

		while self.response is None:
			# print("Waiting..")
			self.connection.process_data_events()
		
		# print(self.response)
		return self.response
	
	"""
	This function is triggered whenever a message is recieved in the responseQ or the writeQ
	It checks that the reply message is for the request was sent by comparing the correlation id
	set by the sendMessage function and sets the value to the self.response so that he sendMessage
	ends and returns.
	"""
	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			ch.basic_ack(delivery_tag=method.delivery_tag)
			print("got Response")
			self.response = body
			print("Sent ACK")
		else:
			print("Recieved a Message")

"""
This function is used to stop a Worker container.

This is important to note that it is an intentional stopping of  a
worker ( used in scaling process) so when this function is called
it is NOT treated as worker CRASH and no new Container is spawned 
as per the fault tolerance policy.


The Worker which has the highest PID is always stopped this ensures that
it is not a Master which is stopped.

A Constraint has been added that if only two containers are active (i.e Master and Slave)
no more containers can be stopped since this would lead to the service being unavailable.   

The code first identifies which container needs to be stopped be fetching the PID's of 
all the working containers from '/Container_pid' child znodes.

After getting the container ID of the worker the code also fetches the mongoDB container ID
which is present is the child of Container ID znode and stops them.

The stopping process first involves deleting the container node from '/Container_pid' child
this ensures that a new container as per the  fault tolerance polcy is not spawned.
"""
def stop_worker():
	client = docker.from_env()
	
	running_containers = zk.get_children("/Container_pid")
	if(len(running_containers)  == 2):
			return False

	mapping = dict()
	for container in running_containers:
		pid = int(zk.get("/Container_pid/" + container)[0].decode('utf-8'))
		mapping[pid] = container

	container_pid = max(mapping.keys())

	container_id = mapping[container_pid]
	
	print("Stopping -",container_id)
	logging.info("Stopping - %s",container_id)
	mongoContainer = zk.get_children("/Container_pid/" + container_id)[0] 
	zk.delete("/Container_pid/" + container_id,recursive=True)
	client.containers.get(container_id).stop()
	client.containers.get(mongoContainer).stop()
	client.containers.prune()
	return container_pid


"""
The code starts a new Worker.
The worker container along with it's exclusive MongoDB container is started. both of them are
connected to a separate bridge network exclusive to accessible to both of them only.
Also for the worker to communicate with RabbitMQ and Zookeeper it connected to a bridge network created
called as "orch_net", this network connectes all the workers and the orchestrator to the RabbitMQ and
Zookeeper. 

The startup procedure involves Creating a Znode in '/Container_pid' for the container started up with its 
PID and mongoDB container ID so that the container can access its PID for Leader Election Process.
"""
def start_worker():
	client = docker.from_env()
	mongoContainer = client.containers.run('mongo',detach=True)
	# worker = client.containers.run("worker",command=['python','worker.py'],links={mongoContainer.id:"mongodb",rabbit.id:"rabbitmq",zookeeper.id:"zookeeper"},restart_policy={"Name":"on-failure"},detach=True)
	worker = client.containers.create("worker",command=['python','worker.py'],links={mongoContainer.id:"mongodb"},detach=True)
	client.networks.get("orch_net").connect(worker.id)
	logging.info("Worker - %s Created",worker.short_id)
	worker.start()
	pid = worker.top()['Processes'][0][1]
	zk.create('/Container_pid/'+worker.short_id,str(pid).encode('utf-8'))
	zk.create('/Container_pid/'+worker.short_id + '/' + mongoContainer.short_id)
	return (worker.short_id,pid)


#FLask app
app = Flask(__name__)

#Connect to MONGODB container for the counting HTTP requests.
mongo_client = MongoClient("mongodb://"+ MONGO_HOST + ":27017")

#Initialize the count value as 0 and the flag value to False indicating the Scaling Process is not active.
db = mongo_client.orchdata
db['counter'].insert_one({"count":0,"flag":False})

#Connect to Zookeeper
zk = KazooClient(hosts = ZOOKEEPER_HOST+":2181",timeout=10)
zk.start()

# def my_listener(state):
# 	# global pid
# 	if state == KazooState.LOST:
# 		print("lost")
# 	elif state == KazooState.SUSPENDED:
# 		#Handle being disconnected from Zookeeper
# 		print("Suspended")
# 	else:
# 		#Handle being connected/reconnected to Zookeeper
# 		print("connected")


# zk.add_listener(my_listener)

#Ensure the paths required by the service are present in Zookeeper
zk.ensure_path("/Election/Slaves")
zk.ensure_path("/Container_pid/")

#Connecting to RabbitMQ server to faciliate communication with the Workers.
rabbit_client = rabbitmqClient()

#Initially start atleast two containers - Master and Slave. 
start_worker()
start_worker()


"""
Sets a ChildWatch on '/Election/Slaves' which contains the emphemeral nodes of all the
workers so that when a container crashes( leading to the emphmeral node being deleted)
the orchestrator gets notified and as per the Fault -Tolerance policy spawns a new container


"""
@zk.ChildrenWatch("/Election/Slaves", send_event = True)
def watch_parent_node(children, event):
	if(event !=None):
		n_workers = set(zk.get_children("/Container_pid"))
		if(len(children) < len(n_workers) ):
			print("Slave is has been deleted")
			removed_container =	n_workers.difference(set(children)).pop()
			mongoContainer = zk.get_children("/Container_pid/" + removed_container)[0]
			client.containers.get(mongoContainer).stop()
			zk.delete("/Container_pid/" + removed_container,recursive= True)
			start_worker()


"""
Function to handle a write requests from the client ,
send the request to the Master, recieve a response 
and send appropriate response.

"""
@app.route("/api/v1/write",methods=["POST"])
def write_db():
	print("Recieved a Write Request")
	response = json.loads(rabbit_client.sendMessage('write',request.data,rabbit_client.responseQ))

	if(response['status_code'] in [400,405]):
		abort(response['status_code'])

	else:	
		return ("",response['status_code'])


"""
Function to handle a read requests from the client ,
send the request to a Slave, recieve a response 
and send appropriate response and Data.

As soon as a the first request is recieved this function starts the scaling process.

"""
@app.route("/api/v1/read",methods=["POST"])
def read_db():
	print("Recieved a Read Request")
	if(get_flag() == False): # Check if scaling process is started or not
		threading.Thread(target=scale).start() # start scaling 
		update_flag() # update flag to indicate scaling process has started.
	
	inc_counter() # increment read request HTTP count.
	
	#Send read request to Slave
	response = json.loads(rabbit_client.sendMessage('read',request.data,rabbit_client.responseQ))
	
	if(response['status_code'] in [400]):
		abort(response['status_code'])
	
	else:
		return (response['data'],response['status_code'])



"""
This function emulates crashing of a master

It finds the pid of the master from '/Election/Master' searches the container ID for 
the master from '/Election/Slave" and stops the worker container.


"""
@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
	logging.info("Request to Crash Master")
	master_pid = int(zk.get("/Election/Master")[0].decode('utf-8'))
	running_containers = zk.get_children("/Election/Slaves")


	for container in running_containers:
		container_pid = int(zk.get("/Container_pid/" + container)[0].decode('utf-8'))
		if(master_pid == container_pid):
			client.containers.get(container).stop()
			break
	
	return jsonify([master_pid]) 		

"""
This  function emulates crashing of a slave

It finds the container with highest PID and stops that Slave.
 
"""
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
	logging.info("Request to Crash Slave")
	running_containers = zk.get_children("/Election/Slaves")
	
	mapping = dict()

	for container in running_containers:
		container_pid = int(zk.get("/Election/Slaves/" + container)[0].decode('utf-8'))
		mapping[container_pid] = container

	highest_pid = max(mapping.keys())
	client.containers.get(mapping[highest_pid]).stop()
		
	return jsonify([highest_pid])


"""
This function returns the list of all the Workers running.

"""
@app.route("/api/v1/worker/list",methods=["GET"])
def list_worker():
	logging.info("Request to List all workers")
	running_containers = zk.get_children("/Container_pid")
	pid_list = []
	for container in running_containers:
		container_pid = int(zk.get("/Container_pid/" + container)[0].decode('utf-8'))
		pid_list.append(container_pid)
	pid_list.sort()
	return jsonify(pid_list) 		
	

"""
This functions deletes all the data present in the rideshare database.


"""
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	
	data_to_send = {}
	data_to_send['table'] = 'users'
	data_to_send['operation'] = 'delete'
	data_to_send['data'] = {}

	response = json.loads(rabbit_client.sendMessage('write',json.dumps(data_to_send),rabbit_client.responseQ))
	

	data_to_send['table'] = 'rides'

	response = json.loads(rabbit_client.sendMessage('write',json.dumps(data_to_send),rabbit_client.responseQ))

	return ""

if __name__ == '__main__':
	app.debug =True	
	app.run(host="0.0.0.0",use_reloader = False)  #Threaded to have Mutliple concurrent requests


