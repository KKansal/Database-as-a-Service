import pika
from pymongo import MongoClient,errors
from json import dumps,loads

client = MongoClient("mongodb://localhost:27017")

db = client.rideshare
#db.rides_counter_table.delete_many({})
#db.rides_counter_table.insert_one({"http_counter":0})

db.rides.create_index([('rideId',1)],unique=True)

"""
def write_db(channel,message,properties,body):
	print(" [x] Received Write request%r" % body)
	json_data = loads(body)
	try:
		collection_name = json_data['table']
		document =  json_data['data']
		if(json_data['operation']=="update"):
			filter_data = json_data['filter']
	except KeyError:
		pass
		#TODO	abort(400) # Recieved Data is not valid  - BAD Request

	if(json_data['operation']=="insert"):
		success_code = insert_data(collection_name,document)
	elif(json_data['operation']=="delete"):
		success_code = delete_data(collection_name,document)
	elif(json_data['operation']=="update"):
		success_code = update_data(collection_name,document,filter_data)
	else:
		pass
	#TODO	abort(400) #Operation not supported - BAD Request
	if(success_code==1):
		pass
		#TODO return empty_response
	else:
		pass
		#TODO abort(405) #Error Encountered while Performing DB operations - Method Not Allowed

def update_data(collection_name,document,filter_data):

	print("Updating based on",document,"for ONE row satisfying",filter_data)
	try:
		db[collection_name].update(filter_data,document)
		return 1
	except:
		print("correct")
		return 0


def insert_data(collection_name,document):
	print("Inserting",document,"into collection",collection_name)
	try:
		db[collection_name].insert_one(document)
		return 1
	except errors.DuplicateKeyError:
		print("DuplicateKeyError")
		return 0

def delete_data(collection_name,document):
	print("Deleting",document,"from collection",collection_name)
	print("Searching",document,"from collection",collection_name)
	search_res = db[collection_name].find_one(document)
	#print(document)

	print(search_res)
	if(search_res==None):
		print("here")
		return 0
	try:
		db[collection_name].delete_one(document)
		print("Operation Done")
		return 1
	except:
		return 0
"""


"""
9. Read from DB
POST Request
Body Format
{
	"table":"tablename",
	"conditions":{column:value , ...}
}

"""





class rabbitmqServer():
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

		self.channel = self.connection.channel()

		self.channel.exchange_declare(exchange='readWrite',exchange_type='direct')

		self.channel.queue_declare(queue='readQ')

		self.channel.queue_declare(queue="responseQ")

		#if master then only
		# self.channel.queue_declare(queue="writeQ")


		# self.channel.queue_bind(exchange='readWrite', queue='writeQ',routing_key='write')

		#endif

		self.channel.queue_bind(exchange='readWrite', queue='readQ',routing_key='read')

		self.channel.queue_bind(exchange="readWrite",queue="responseQ")

		self.channel.basic_consume(queue='readQ', on_message_callback=self.read_db)

		# self.channel.basic_consume(queue='writeQ', on_message_callback=write_db)

		print(' [*] Waiting for messages. To exit press CTRL+C')
		self.channel.start_consuming()

	def read_db(self,channel,method,props,body):
		print(" [x] Received Read Request for %r" % body)
		data_request = loads(body)
		response = {"status_code":None,"data":{}}
		try:
			collection = data_request['table']
			condition = data_request['conditions']
		except KeyError:
			print("KeyError-Not all fields are present")
			response["status_code"] = 400
			return self.sendResponse(dumps(response),method,props)

		
		cursor = db[collection].find(condition)
		
		if((cursor.count())>1):
			print("WARNING :The Query matches",cursor.count(),"documents")
		
		elif(cursor.count()==0):
			print("WARNING : 0 results matched")
			response["status_code"] = 204
			return self.sendResponse(dumps(response),method,props)
			
		res = list()
		
		for row in cursor:
			row.pop("_id")
			res.append(row)
		print(res)

		response["status_code"] = 200
		response["data"] = dumps(res)
		return self.sendResponse(dumps(response),method,props)

	def sendResponse(self,response,method,props):
			self.channel.basic_publish(exchange='readWrite',routing_key=props.reply_to,
                     					properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     					body=str(response))
			self.channel.basic_ack(delivery_tag=method.delivery_tag)
			print("Response Sent")	

server = rabbitmqServer()







