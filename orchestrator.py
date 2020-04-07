import pika
from flask import Flask,request,jsonify,abort

app = Flask(__name__)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


channel.exchange_declare(exchange='readWrite',
                         exchange_type='direct')


def sendMessage(routing_key,message):
	channel.basic_publish(exchange='readWrite',
                      	routing_key=routing_key,
                      	body=message)


@app.route("/api/v1/write",methods=["POST"])
def write_db():
	print("Recieved a Write Request")
	sendMessage('write',request.data)

	return " "


@app.route("/api/v1/read",methods=["POST"])
def read_db():
	print("Recieved a Read Response")
	sendMessage('read',request.data)
	
	return " "

if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0",threaded=True)  #Threaded to have Mutliple concurrent requests

connection.close()



"""
8.Write a DB

POST Request
Body Format FOR insert update
{
	"operation":"insert","update"
	"data":data  // A row in JSON format
	"table" :"tablename" 
}

Body Format
{
	"operation":"update"
	"data":"data_to_Be updated"
	"table":"table_name"
	"filter":"filter for documents"


}
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
