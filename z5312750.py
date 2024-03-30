#! /usr/bin/env python3
# -*- coding: utf-8 -*-


# You can import more modules from the standard library here if you need them
# (which you will, e.g. sqlite3).
import os
from pathlib import Path
from flask import Flask, request, jsonify, Response
import sqlite3 as sql
from flask_restx import Api, Resource, Namespace, fields, reqparse
import requests
from datetime import datetime
import json
import re
from dotenv import load_dotenv          
import google.generativeai as genai 

app = Flask(__name__)
api = Api(app, title=' Deutsche Bahn API',
          description='A simple API to query information about Deutsche Bahn trains. Sophia Cibei z5312750')

studentid = Path(__file__).stem         # Will capture your zID from the filename.
db_file   = f"{studentid}.db"           # Use this variable when referencing the SQLite database file.
txt_file  = f"{studentid}.txt"          # Use this variable when referencing the txt file for Q7.

# Load the environment variables from the .env file
load_dotenv()

# Configure the API key
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# Create a Gemini Pro model
gemini = genai.GenerativeModel('gemini-pro')


# Function to create SQLite database and table
def create_database():
    conn = sql.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS locations
                      (location_id INTEGER PRIMARY KEY,
                       name TEXT,
                       latitude REAL, 
                       longitude REAL, 
                       next_departure TEXT, 
                       last_updated TEXT,
                       link_self_href TEXT,
                       UNIQUE(location_id))''')
    conn.commit()
    conn.close()

# Check if the database file already exists
if not os.path.exists(db_file):
    create_database()

# Function to connect to the SQLite database
def connect_to_database():
    return sql.connect(db_file)

def check_in_db(table_name, id_to_check):
    # Connect to the database
    conn = sql.connect(db_file)
    cursor = conn.cursor()

    # Execute the query to check the existence of the ID
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE location_id = ?", (id_to_check,))
    result = cursor.fetchone()[0]

    # Close the database connection
    conn.close()

    # Return True if the ID exists, False otherwise
    return result > 0

# Update the next_departure field for a specific location ID
def update_next_departure(location_id, new_next_departure):
    conn = sql.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE locations SET next_departure = ? WHERE location_id = ?", (new_next_departure, location_id))
        conn.commit()
        print("Next departure updated successfully.")
    except sql.Error as e:
        print("Error updating next departure:", e)
    
    conn.close()

def get_stop_info_db(location_id):
    conn = sql.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                location_id, 
                last_updated, 
                name, 
                latitude, 
                longitude, 
                next_departure
            FROM 
                locations
            WHERE 
                location_id = ?
        """, (location_id,))
        # Fetch the result
        result = cursor.fetchone()
    except sql.Error as e:
        print("Error updating next departure:", e)

    conn.close()
    return result

def get_prev_next(location_id):
    conn = sql.connect(db_file)
    cursor = conn.cursor()
    try:
        # Get the row above the provided location ID
        cursor.execute("""
            SELECT 
                location_id
            FROM 
                locations
            WHERE 
                location_id < ?
            ORDER BY 
                location_id DESC
            LIMIT 1
        """, (location_id,))
        row_above = cursor.fetchone()

        # Get the row below the provided location ID
        cursor.execute("""
            SELECT 
                location_id
            FROM 
                locations
            WHERE 
                location_id > ?
            ORDER BY 
                location_id ASC
            LIMIT 1
        """, (location_id,))
        row_below = cursor.fetchone()
    except sql.Error as e:
        print("Error updating next departure:", e)
    conn.close()
    return row_above if row_above else None, row_below if row_below else None

def delete_record(table_name, record_id):
    conn = sql.connect(db_file)
    cursor = conn.cursor()

    try:
        # Check if the record exists
        cursor.execute(f"SELECT * FROM {table_name} WHERE location_id = ?", (record_id,))
        existing_record = cursor.fetchone()

        if existing_record:
            # If the record exists, delete it
            cursor.execute(f"DELETE FROM {table_name} WHERE location_id = ?", (record_id,))
            success = True
        else:
            success = False

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()
    except sql.Error as e:
        print("Error deleting record:", e)
        conn.rollback()
        conn.close()
    
    return success

def is_valid_datetime_format(value):
    # Define the regular expression pattern for the format "yyyy-mm-ddhh:mm:ss"
    pattern = r'^\d{4}-\d{2}-\d{2}\d{2}:\d{2}:\d{2}$'

    # Check if the value matches the pattern
    if re.match(pattern, value):
        return True
    else:
        return False

def update_field_in_db(table, field, value, condition_field, condition_value):
    conn = sql.connect(db_file)
    cursor = conn.cursor()

    # Update the field with the new value based on the condition
    cursor.execute(f"UPDATE {table} SET {field} = ? WHERE {condition_field} = ?", (value, condition_value))
    conn.commit()

    conn.close()

def get_field_value_from_db(table, field, condition_field, condition_value):
    conn = sql.connect(db_file)  
    cursor = conn.cursor()

    # Execute the SELECT query to retrieve the value of the specified field
    cursor.execute(f"SELECT {field} FROM {table} WHERE {condition_field} = ?", (condition_value,))
    row = cursor.fetchone()

    if row:
        value = row[0]  # Extract the value from the first column of the result row
    else:
        value = None

    conn.close()
    return value



@api.route('/stops')
class AddStops(Resource):
    @api.doc(params={'query': 'The query string for the stop'})
    @api.response(201, 'CREATED')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Not Found')
    def put(self):
        # Get the value of the 'query' parameter from the request
        query = request.args.get('query')
        print("this is the query", query)

        # Check if the 'query' parameter is present
        if not query:
            return {'error': 'No query provided'}, 400

        # Construct the URL for the external API request with the 'query' parameter value inserted
        api_url = f'https://v6.db.transport.rest/locations?query={query}&results=5'

        # Make a request to the external API
        response = requests.get(api_url)
        print('this is the reqponse', response)

        # Check if the request to the external API was successful
        if response.status_code != 200:
            return {'error': 'Failed to fetch stop data from external API'}, 500

        # Extract stop data from the API response
        stop_data = response.json()
        print('this is the json data', stop_data)

        # Connect to the SQLite database
        conn = connect_to_database()
        cursor = conn.cursor()

        # Iterate over the stop data and insert into the SQLite database
        sorted_stop_data = sorted(stop_data, key=lambda x: int(x['id']))
        for item in sorted_stop_data:
            item_id = item['id']
            item_name = item['name']
            item_latitude = item['location']['latitude']
            item_longitude = item['location']['longitude']
            # Check if the stop_id already exists in the database
            cursor.execute("SELECT location_id FROM locations WHERE location_id=?", (item_id,))
            existing_stop = cursor.fetchone()
            if existing_stop:
                # Update the last_updated timestamp if the stop already exists
                cursor.execute("UPDATE locations SET last_updated = datetime('now'), \
                name = ?, \
                latitude = ?, \
                longitude = ? \
                WHERE location_id = ?", (item_name, item_latitude, item_longitude, item_id))

                print('existing')

            else:
                # Insert the stop data into the database if it doesn't exist
                link_self_href =  f"http://localhost:8888/stops/{item_id}"
                cursor.execute("INSERT INTO locations (location_id, last_updated, name, latitude, longitude, link_self_href) VALUES (?, datetime('now'), ?, ?, ?, ?)",
                (item_id, item_name, item_latitude, item_longitude, link_self_href))
                print('new location')
        # Commit changes and close connection
        conn.commit()
        conn.close()

        # Construct the response in the desired format
        response_data = [
            {
                "stop_id": stop['id'],
                "last_updated": datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
                "_links": {
                    "self": {
                        "href": f"http://localhost:8888/stops/{stop['id']}"
                    }
                }
            }
            for stop in sorted_stop_data
        ]
        print(response_data)
        # Convert the response_data to JSON string
        json_data = json.dumps(response_data)

        # Create a Flask Response object
        response = Response(response=json_data, status=201, mimetype='application/json')

        # Return the Flask Response object
        return response
    


@api.route('/stops/<stop_id>')
class Stop(Resource):
    @api.doc(params={'include': 'Fields to include in the response'})
    @api.response(200, 'OK')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Not Found')
    def get(self, stop_id):
        # query = request.args.get('stop_id')
        query = stop_id
        print("this is the query", query)

        # Check if the 'query' parameter is present
        if not query:
            return {'error': 'No query provided'}, 400
        id = query
        # check stop id in db
        if not check_in_db("locations", id):
            return {'error': 'Stop not in database'}, 400
        
        # Construct the URL for the external API request with the 'query' parameter value inserted
        api_url = f'https://v6.db.transport.rest/stops/{id}/departures?duration=120'

        # Make a request to the external API
        response = requests.get(api_url)
        print('this is the reqponse', response)

        # Check if the request to the external API was successful
        if response.status_code != 200:
            return {'error': 'Failed to fetch stop data from external API'}, 500

        # Extract stop data from the API response
        platform = None
        direction = None
        response = response.json()
        for entry in response['departures']:
            if entry.get('platform') is not None and entry.get('direction') is not None:
                print('not null!!!!!!')
                platform = entry['platform']
                direction = entry['direction']
                break
        
        print('platform and direction', platform, direction)
        if platform is None or direction is None:
            return {'error': 'No next departure found for this stop'}, 404
        # update the db
        next_departure = f'Platform {platform} towards {direction}'
        update_next_departure(id, next_departure)

        # provide response
        # Construct the response in the desired format
        stop_info = get_stop_info_db(id)
        # Unpack the result tuple into separate variables
        if stop_info:
            stop_id, last_updated, name, latitude, longitude, next_departure = stop_info
        else:
            print("No information found for location ID:", id)
        
        # get prev and next 
        prev, next = get_prev_next(id)
        response_data = {
                "stop_id": stop_id,
                "last_updated": last_updated,
                "name": name, 
                "latitude": latitude, 
                "longitude": longitude, 
                "next_departure": next_departure, 
                "_links": {
                    "self": {
                        "href": f"http://localhost:8888/stops/{stop_id}"
                    }, 
                }
        }
        # Parse the 'include' query parameter
        include_fields = request.args.get('include', '').split(',')
        print('include!!', include_fields)
    
        # Include fields in the response data based on the 'include' query parameter
        if include_fields[0] != '':
            if 'name' not in include_fields:
                response_data.pop('name', None)
            if 'latitude' not in include_fields:
                response_data.pop('latitude', None)
            if 'longitude' not in include_fields:
                response_data.pop('longitude', None)
            if 'next_departure' not in include_fields:
                response_data.pop('next_departure', None)
        if prev:
            response_data['_links']['prev'] = {'href': f"http://localhost:8888/stops/{prev[0]}"}
        if next:
            response_data['_links']['next'] = {'href': f"http://localhost:8888/stops/{next[0]}"}
        print(response_data)
        # Convert the response_data to JSON string
        json_data = json.dumps(response_data)

        # Create a Flask Response object
        response = Response(response=json_data, status=200, mimetype='application/json')
        # Return the Flask Response object
        return response
    @api.response(200, 'OK')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Not Found')
    def delete(self, stop_id):
        if not check_in_db("locations", stop_id):
            return {'message': f'the stop_id {stop_id} was not found in the database.',
                    'stop_id': stop_id}, 404        
        success = delete_record("locations", stop_id)
        if success:
            return {'message': f'the stop_id {stop_id} was removed from the database.',
                    'stop_id': stop_id}, 200
        else:
            return {'message': f'error'}, 404  

    @api.expect(api.model('UpdateStopRequest', {
        'last_updated': fields.String(description='Time last updated'),
        'name': fields.String(description='Name of the stop'),
        'latitude': fields.Float(description='Latitude of the stop'),
        'longitude': fields.Float(description='Longitude of the stop'),
        'next_departure': fields.String(description='Next departure')
    }))
    def patch(self, stop_id):
        if not check_in_db("locations", stop_id):
            return {"error": "Stop not found"}, 404

        update_fields = request.json
        if not update_fields:
            return {"error": "No fields provided for update"}, 400

        print('here', update_fields)

        # check valid input for fields
        for key in update_fields.keys():
            if update_fields[key] == '':
                return  {"error": f"{key} is empty"}, 400
            if key == 'latitude':
                if update_fields[key] < -90 or update_fields[key] > 90:
                    return  {"error": f"{update_fields[key]} is an invalid input for {key}"}, 400
            if key == 'longitude':
                if update_fields[key] < -180 or update_fields[key] > 180:
                    return  {"error": f"{update_fields[key]} is an invalid input for {key}"}, 400
            if key == 'last_updated':
                if not is_valid_datetime_format(update_fields[key]):
                    return  {"error": f"{update_fields[key]} is an invalid format for {key}"}, 400

        # update values
        for key in update_fields.keys():
            update_field_in_db("locations", key, update_fields[key], "location_id", stop_id)
        if 'last_updated' not in update_fields.keys():
            update_field_in_db("locations", 'last_updated', datetime.now().strftime("%Y-%m-%d-%H:%M:%S"), "location_id", stop_id)
        # Construct the response in the desired format
        last_updated = get_field_value_from_db("locations", "last_updated", 'location_id', stop_id)

        response_data = {
                "stop_id": stop_id,
                "last_updated": last_updated,
                "_links": {
                    "self": {
                        "href": f"http://localhost:8888/stops/{stop_id}"
                    }
                }
        }

        # Convert the response_data to JSON string
        json_data = json.dumps(response_data)

        # Create a Flask Response object
        response = Response(response=json_data, status=200, mimetype='application/json')

        # Return the Flask Response object
        return response

@api.route('/stops/<stop_id>/departures')
class Stop(Resource):
    @api.response(200, 'OK')
    @api.response(400, 'Bad Request')
    @api.response(404, 'Not Found')
    def get(self, stop_id):
        
        # check stop in db
        if not check_in_db("locations", stop_id):
            return {"error": "Stop not found"}, 404
        # use api for this stop
        # Construct the URL for the external API request with the 'query' parameter value inserted
        api_url = f'https://v6.db.transport.rest/stops/{stop_id}/departures?duration=90'

        # Make a request to the external API
        response = requests.get(api_url)
        print('this is the reqponse', response)

        # Check if the request to the external API was successful
        if response.status_code != 200:
            return {'error': 'Failed to fetch stop data from external API'}, 500

        response_data = response.json()

        # get info for each operator
        operator_names = []
        for result in response_data['departures']:
            if 'line' in result and 'operator' in result['line']:
                if result['line']['operator']['name'] not in operator_names:
                    operator_names.append(result['line']['operator']['name'])
            
            if len(operator_names) == 5:
                break
        
        print(operator_names)
        operator_info = {}
        for operator in operator_names:
            question = f"Give me a summary about the Dautsche Bahn operator {operator}!"
            response = gemini.generate_content(question)
            operator_info[operator] = response.text.replace('**', '').replace('\n', ' ')

        # Construct the response in the desired format
        answer_data = {
                "stop_id": stop_id,
                "profiles": [
                    {
                        "operator_name": operator, 
                        "information": operator_info[operator]
                    }
                    for operator in operator_names
                ]
        }
        # Convert the response_data to JSON string
        json_data = json.dumps(answer_data)

        # Create a Flask Response object
        response = Response(response=json_data, status=200, mimetype='application/json')

        # Return the Flask Response object
        return response





        




if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8888, debug=True)


######################################################################################





# if __name__ == "__main__":
#     # Here's a quick example of using the Generative AI API:
#     question = "Give me some facts about UNSW!"
#     response = gemini.generate_content(question)
#     print(question)
#     print(response.text)