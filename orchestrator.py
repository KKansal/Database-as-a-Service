from flask import Flask,request,jsonify,abort
from kazoo.client import KazooClient
from kazoo.client import KazooState
import docker
import json
import pika
import uuid
import logging



# logging.basicConfig()

client = docker.from_env()
zookeeper = client.containers.run("zookeeper",detach=True,ports={2181:2181,3888:3888,8080:8080})

rabbit = client.containers.run("rabbitmq:3-management",
								hostname='rabbitmq',
								volumes = {'rabbitmq':{'bind':"/var/lib/rabbitmq",'mode':'rw'}},
								detach=True,ports={15672:15672,5672:5672})


class rabbitmqClient():

	def __init__(self):
		# Set up Connection
		self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',heartbeat=0))
		self.channel = self.connection.channel()

		# Declare Exchange
		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')

		# Declare Sync Exchange
		self.channel.exchange_declare(exchange='sync',exchange_type='fanout')

		#Declare Eat-UP Queue
		self.channel.queue_declare(queue="eatQ",durable=True)

		# Declare readQ
		self.channel.queue_declare(queue='readQ')

		# Declare writeQ
		self.channel.queue_declare(queue="writeQ")

		#Declare  Response Queues
		responseQ = self.channel.queue_declare(queue="responseQ")
		self.responseQ = responseQ.method.queue

		writeResponseQ = self.channel.queue_declare(queue="writeResponseQ")
		self.writeResponseQ = writeResponseQ.method.queue

		self.channel.queue_bind(exchange='readWrite', queue='writeQ',routing_key='write')

		self.channel.queue_bind(exchange="readWrite", queue=self.responseQ)

		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')

		self.channel.queue_bind(exchange = 'readWrite' , queue = self.writeResponseQ)

		self.channel.queue_bind(exchange="sync",queue='eatQ')

		self.channel.basic_consume(
            queue=self.responseQ,
            on_message_callback=self.on_response)

		self.channel.basic_consume(
            queue=self.writeResponseQ,
            on_message_callback=self.on_response)
		
		# self.channel.start_consuming()
		

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

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			ch.basic_ack(delivery_tag=method.delivery_tag)
			print("got Response")
			self.response = body
			print("Sent ACK")
		else:
			print("Recieved a Message")




def launch_worker(client):
	mongoContainer = client.containers.run('mongo',detach=True)
	worker = client.containers.run("worker",command=['python','worker.py'],links={mongoContainer.id:"mongodb",rabbit.id:"rabbitmq",zookeeper.id:"zookeeper"},restart_policy={"Name":"on-failure"},detach=True)
	logging.info("Worker - %s Created",worker.short_id)
	pid = worker.top()['Processes'][0][1]
	zk.create('/Container_pid/'+worker.short_id,str(pid).encode('utf-8'))
	return (worker.short_id,pid)

app = Flask(__name__)




import time 
time.sleep(20)

zk = KazooClient(hosts = "127.0.0.1:2181",timeout=10)

def my_listener(state):
	# global pid
	if state == KazooState.LOST:
		print("lost")
	elif state == KazooState.SUSPENDED:
		#Handle being disconnected from Zookeeper
		print("Suspended")
	else:
		#Handle being connected/reconnected to Zookeeper
		print("connected")


zk.add_listener(my_listener)
zk.start()

zk.ensure_path("/Election/Slaves")
zk.ensure_path("/Container_pid/")

rabbit_client = rabbitmqClient()


launch_worker(client)
launch_worker(client)

@zk.ChildrenWatch("/Election/Slaves", send_event = True)
def watch_parent_node(children, event):
	if(event !=None):
		n_workers = set(zk.get_children("/Container_pid"))
		if(len(children) < len(n_workers) ):
			print("Slave is has been deleted")
			removed_container =	n_workers.difference(set(children)).pop()
			zk.delete("/Container_pid/" + removed_container)
			#Launch a node
			launch_worker(client)
		else:
			print("Slave is added")
			

@app.route("/api/v1/write",methods=["POST"])
def write_db():
	print("Recieved a Write Request")
	response = json.loads(rabbit_client.sendMessage('write',request.data,rabbit_client.responseQ))

	if(response['status_code'] in [400,405]):
		abort(response['status_code'])

	else:	
		return ("",response['status_code'])


@app.route("/api/v1/read",methods=["POST"])
def read_db():
	print("Recieved a Read Request")

	response = json.loads(rabbit_client.sendMessage('read',request.data,rabbit_client.responseQ))
	
	if(response['status_code'] in [400]):
		abort(response['status_code'])
	
	else:
		return (response['data'],response['status_code'])

#TODO
@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
	logging.info("Request to Crash Master")
	master_pid = int(zk.get("/Election/Master")[0].decode('utf-8'))
	running_containers = zk.get_children("/Election/Slaves")
	if(len(running_containers == 2)):
		abort(405)

	for container in running_containers:
		container_pid = int(zk.get("/Nodes/" + container)[0].decode('utf-8'))
		if(master_pid == container_pid):
			zk.delete("/Nodes/"+container)
			client.containers.get(container).stop()
			break
	return jsonify([master_pid]) 		
#TODO
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
	logging.info("Request to Crash Slave")
	running_containers = zk.get_children("/Nodes")
	if(len(running_containers == 2)):
		abort(405)	
	mapping = dict()

	for container in running_containers:
		container_pid = int(zk.get("/Nodes/" + container)[0].decode('utf-8'))
		mapping[container_pid] = container

	highest_pid = max(mapping.keys())
	zk.delete("/Nodes/"+mapping[highest_pid])
	client.containers.get(mapping[highest_pid]).stop()
		
	return jsonify([highest_pid]) 		



@app.route("/api/v1/worker/list",methods=["POST"])
def list_worker():
	logging.info("Request to List all workers")
	running_containers = zk.get_children("/Nodes")
	pid_list = []
	for container in running_containers:
		container_pid = int(zk.get("/Nodes/" + container)[0].decode('utf-8'))
		pid_list.append(container_pid)
	pid_list.sort()
	return jsonify(pid_list) 		
	






if __name__ == '__main__':
	app.debug =True	
	app.run(use_reloader = False)  #Threaded to have Mutliple concurrent requests


