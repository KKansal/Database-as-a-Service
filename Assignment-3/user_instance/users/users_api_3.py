from werkzeug.exceptions import MethodNotAllowed
from flask import Flask,request,jsonify,abort
from pymongo import MongoClient,errors
import requests as send_request
import json
from flask import make_response
from datetime import datetime
import pandas as pd

users_mongo_client_port = 27017

df = pd.read_csv("AreaNameEnum.csv")
area_dict = dict()

for i in range(len(df)):
	area_dict[df['Area No'][i]]=df['Area Name'][i]

app = Flask(__name__)
client = MongoClient('mongodb://mongodb:27017')
client.users_database.users.create_index('username',unique=True)


db = client.users_database
db.users_counter_table.delete_many({})
db.users_counter_table.insert_one({"http_counter":0})


empty_response = app.make_response('{}') #default 200 OK send this response for sending 200 OK



def increment_counter():
	data_to_send = dict()
	data_to_send['table'] = 'users_counter_table'
	data_to_send['operation'] = 'update'
	data_to_send['data'] = {'$inc': {'http_counter':1}}
	data_to_send['filter'] = {}

	response_recieved = send_request.post('http://127.0.0.1:5000/api/v1/write',json=data_to_send)


@app.route("/api/v1/_count",methods = ["GET","DELETE"])
def total_requests():
	
	if request.method == 'GET':
		
		data_to_send = dict()
		data_to_send['table'] = 'users_counter_table'
		data_to_send['conditions'] = {}

		response_recieved = send_request.post('http://127.0.0.1:5000/api/v1/read',json = data_to_send)

		data_recieved = response_recieved._content

		json_data = json.loads(data_recieved)
		counter = json_data[0]['http_counter']

		
		if(response_recieved.status_code == 200):
			return jsonify([counter])
	
	else:

		data_to_send = dict()
		data_to_send['table'] = 'users_counter_table'
		data_to_send['operation'] = 'update'
		data_to_send['data'] = {"http_counter":0}
		data_to_send['filter'] = {}

		response_recieved = send_request.post('http://127.0.0.1:5000/api/v1/write',json=data_to_send)

		print(response_recieved.status_code)

		if(response_recieved.status_code == 200):
			response_200 = make_response('{}', 200)
			return response_200

				


@app.route("/api/v1/users",methods=["GET"])
def list_users():

	increment_counter()

	res=[]
	mycol = db["users"]
	
	for x in mycol.find({},{ "username": 1 }):
	  res.append(x['username'])

	if(len(res) == 0):
		response_204 = make_response(jsonify(res),204)
		return response_204
		  
	return jsonify(res)
	
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():	
	mycol = db["users"]
	x = mycol.delete_many({})

	return empty_response
	




"""
1.Add Users

POST Request
Body Format
{
	"username":"username",
	"password":"password"

}

"""
hashset = set(['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'])
@app.route("/api/v1/users",methods=["PUT"])
def add_users():

	increment_counter()

	print("recieved a request to add user")
	user_details = request.get_json()
	data_to_send = dict()
	data_to_send['operation'] = 'insert'
	data_to_send['table'] ='users'
	data_to_send['data'] = user_details
	print(user_details)

	try:
		print("here")
		if(len(user_details['password'])==40):
			if((hashset & set(user_details['password'])) != set(user_details['password'])):
				print("Password not in SHA1")
				abort(400)
		else:
			print("Password not in 40 length")
			abort(400)
	except:
			abort(400)
	
	print("Generating Request")
	print(data_to_send)
	response_recieved = send_request.post('http://127.0.0.1:5000/api/v1/write',json=data_to_send)
	print("Response Recieved")
	
	response_201 = make_response('{}',201)
	if(response_recieved.status_code == 200):
		return response_201
	else:
		abort(400)

"""
2. Remove a user form db
"""	
@app.route("/api/v1/users/<username>",methods=["DELETE"])
def delete_users(username):

	increment_counter()
	
	print("Recieved request to delete the user with username:",username)

	data_to_send = dict()
	data_to_send['table'] = 'users'
	data_to_send['operation'] = 'delete'
	data_to_send['data'] = {"username":username}

	response_recieved = send_request.post('http://127.0.0.1:5000/api/v1/write',json = data_to_send)


	response_400 = make_response('{}',400)
	if(response_recieved.status_code  == 405):
		return response_400
	else:
		return empty_response


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
@app.route("/api/v1/write",methods=["POST"])
def write_db():	
	json_data = request.get_json()
	try:
		collection_name = json_data['table']
		document =  json_data['data']
		if(json_data['operation']=="update"):
			filter_data = json_data['filter']
	except KeyError:
		abort(400) # Recieved Data is not valid  - BAD Request

	if(json_data['operation']=="insert"):
		success_code = insert_data(collection_name,document)
	elif(json_data['operation']=="delete"):
		success_code = delete_data(collection_name,document)
	elif(json_data['operation']=="update"):
		success_code = update_data(collection_name,document,filter_data)
	else:
		abort(400) #Operation not supported - BAD Request
	if(success_code==1):
		return empty_response
	else:
		abort(405) #Error Encountered while Performing DB operations - Method Not Allowed

def update_data(collection_name,document,filter_data):
	print("Updating based on",document,"for ONE row satisfying",filter_data)
	try:
		db[collection_name].update(filter_data,document)
		return 1
	except:
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
9. Read from DB
POST Request
Body Format
{
	"table":"tablename",
	"conditions":{column:value , ...}
}

"""
@app.route("/api/v1/read",methods=["POST"])
def read_db():
	data_request = request.get_json()
	try:
		collection = data_request['table']
		condition = data_request['conditions']
	except KeyError:
		abort(400)
	cursor = db[collection].find(condition)
	if((cursor.count())>1):
		print("WARNING :The Query matches",cursor.count(),"documents")
	elif(cursor.count()==0):
		print("WARNING : 0 results matched")
		respone_204 = make_response('',204)
		return respone_204

	res = list()
	for row in cursor:
		row.pop("_id")
		res.append(row)
	return jsonify(res)
	# res = cursor[0]                      
	# res.pop("_id")
	# return jsonify(res)

@app.errorhandler(MethodNotAllowed)
def handle_exception(e):
	increment_counter()
	response = e.get_response()
	print(response)
	return response


if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0",threaded=True)  #Threaded to have Mutliple concurrent requests        
