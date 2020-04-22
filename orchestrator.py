from flask import Flask,request,jsonify,abort
import json
import pika
import uuid


app = Flask(__name__)


class rabbitmqClient():

	def __init__(self):
		#Set up Connection
		self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
		self.channel = self.connection.channel()

		#Declare Exchange
		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')

		#Declare readQ
		self.channel.queue_declare(queue='readQ')

		#Declare writeQ
		# writeQ = self.channel.queue_declare(queue="writeQ")
		# self.writeQ = writeQ.method.queue

		#Declare ReadQ
		responseQ = self.channel.queue_declare(queue="responseQ")
		self.responseQ = responseQ.method.queue

		# self.channel.queue_bind(exchange='readWrite', queue='writeQ',routing_key='write')

		self.channel.queue_bind(exchange="readWrite", queue=self.responseQ)

		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')

		self.channel.basic_consume(
            queue=self.responseQ,
            on_message_callback=self.on_response)

		# self.channel.basic_consume(
        #     queue=self.writeQ,
        #     on_message_callback=self.on_response,
        #     auto_ack=True)


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
			self.response = body
			ch.basic_ack(delivery_tag=method.delivery_tag)
		else:
			print("Recieved a Message")


client = rabbitmqClient()


@app.route("/api/v1/write",methods=["POST"])
def write_db():
	# print("Recieved a Write Request")
	# client.sendMessage('write',request.data,client.writeQ)

	return " "


@app.route("/api/v1/read",methods=["POST"])
def read_db():
	print("Recieved a Read Request")

	response = json.loads(client.sendMessage('read',request.data,client.responseQ))
	
	if(response['status_code'] in ['400']):
		abort(response['status_code'])
	
	else:
		return (response['data'],response['status_code'])

if __name__ == '__main__':	
	app.run()  #Threaded to have Mutliple concurrent requests

# connection.close()
