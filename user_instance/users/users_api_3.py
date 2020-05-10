from werkzeug.exceptions import MethodNotAllowed
from flask import Flask,request,jsonify,abort
import requests as send_request
import json
from flask import make_response
from datetime import datetime
import pandas as pd


dbaas_url = "http://18.210.102.92"

df = pd.read_csv("AreaNameEnum.csv")
area_dict = dict()

for i in range(len(df)):
	area_dict[df['Area No'][i]]=df['Area Name'][i]

app = Flask(__name__)

empty_response = app.make_response('{}') #default 200 OK send this response for sending 200 OK



def increment_counter():
	data_to_send = dict()
	data_to_send['table'] = 'users_counter_table'
	data_to_send['operation'] = 'update'
	data_to_send['data'] = {'$inc': {'http_counter':1}}
	data_to_send['filter'] = {}

	response_recieved = send_request.post( dbaas_url +'/api/v1/write',json=data_to_send)


@app.route("/api/v1/_count",methods = ["GET","DELETE"])
def total_requests():
	
	if request.method == 'GET':
		
		data_to_send = dict()
		data_to_send['table'] = 'users_counter_table'
		data_to_send['conditions'] = {}

		response_recieved = send_request.post(dbaas_url + '/api/v1/read',json = data_to_send)

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

		response_recieved = send_request.post( dbaas_url + '/api/v1/write',json=data_to_send)

		print(response_recieved.status_code)

		if(response_recieved.status_code == 200):
			response_200 = make_response('{}', 200)
			return response_200

				


@app.route("/api/v1/users",methods=["GET"])
def list_users():

	increment_counter()

	data_to_send = dict()
	data_to_send['table'] = 'users'
	data_to_send['conditions'] ={}

	response_received = send_request.post(dbaas_url + '/api/v1/read',json = data_to_send)
	
	if(response_received.status_code == 200):
		data = response_received.json()
		
		res = []
		for i in data:
			res.append(i['username'])

		return jsonify(res)
	elif(response_received.status_code == 204):
		return "",204




	# res=[]
	# mycol = db["users"]
	
	# for x in mycol.find({},{ "username": 1 }):
	#   res.append(x['username'])

	# if(len(res) == 0):
	# 	response_204 = make_response(jsonify(res),204)
	# 	return response_204
		  
	# return jsonify(res)
	
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	data_to_send = {}
	data_to_send['table'] = 'users'
	data_to_send['operation'] = 'delete'
	data_to_send['data'] = {}

	response_recieved = send_request.post( dbaas_url + '/api/v1/write',json = data_to_send)
	
	data_to_send = {}
	data_to_send['table'] = 'rides'
	data_to_send['operation'] = 'delete'
	data_to_send['data'] = {}

	response_recieved = send_request.post( dbaas_url + '/api/v1/write',json = data_to_send)
	




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
	response_recieved = send_request.post( dbaas_url + '/api/v1/write',json=data_to_send)
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

	response_recieved = send_request.post( dbaas_url + '/api/v1/write',json = data_to_send)


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

9. Read from DB
POST Request
Body Format
{
	"table":"tablename",
	"conditions":{column:value , ...}
}

"""

@app.errorhandler(MethodNotAllowed)
def handle_exception(e):
	increment_counter()
	response = e.get_response()
	print(response)
	return response


if __name__ == '__main__':	
	app.debug=True
	app.run(host="127.0.0.1",port = 5001,threaded=True)  #Threaded to have Mutliple concurrent requests        
