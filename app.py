from flask import Flask, jsonify, request, render_template
import mysql.connector
from mysql.connector import Error
# -*- coding: utf-8 -*-

app = Flask(__name__)

# Database configuration
db_config = {
    'host': 'localhost',  
    'user': 'root',  
    'password': 'test1',  
    'database': 'weatherAPI'
}

def get_db_connection():
    """Create and return a new database connection."""
    connection = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    return connection

#locations
location_coordinates = {
    '36.3992,25.4793': 'Athens',
    '35.3397,25.1803': 'Heraklion',
    '36.7167,24.45': 'Milos'
}

#######ENDPOINTS########

@app.route("/")
def home():
    return render_template("home.html")

@app.route('/locations', methods=['GET'])
def list_locations():
    #List all unique locations and providing their names.
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT the_coordinates FROM forecast")  # Table name changed to 'forecast'
        result = cursor.fetchall()
        locations=[]
        # Iterate through each row in the result
        for row in result:
            coordinates = row[0]
            location_name = location_coordinates[coordinates]
            locations.append((coordinates, location_name))
        return render_template('locations.html', locations=locations)
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/latest_forecast', methods=['GET'])
def latest_forecast():
    #List the latest forecast for each location for every day.
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        #Grouping by the locations in partions and ordering by date desceding 
        #The outer query then filters this result to include only rows where rn equals 1
        #giving us the latest data for each location on each date.
        cursor.execute("""
            SELECT the_coordinates, the_date, the_temp, the_humidity, the_windspeed
            FROM (
                SELECT 
                    the_coordinates, the_date, the_temp, the_humidity, the_windspeed,
                    ROW_NUMBER() OVER (PARTITION BY the_coordinates, DATE(the_date) ORDER BY the_date DESC) AS rn
                FROM forecast
            ) sub
            WHERE rn = 1;
        """)
        result = cursor.fetchall()
        forecasts = [{
            'coordinates': row[0],
            'date': row[1],
            'temperature': row[2],
            'humidity': row[3],
            'windspeed': row[4]
        } for row in result]
        return render_template('latest_forecast.html', forecasts=forecasts)
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/average_temp', methods=['GET'])
def average_temp():
    #List the average temperature of the last 3 forecasts for each location for every day.
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        #Grouping by the locations in partions and ordering them by date
        #then it takes the 3 rows (latest forecasts) from each partion
        #finaly calculates the average of the 3 temperatures for each location
        query = """
        WITH ranked_forecasts AS ( 
            SELECT 
            the_coordinates,the_date,the_temp,
            ROW_NUMBER() OVER (
                PARTITION BY the_coordinates, DATE(the_date)
                ORDER BY the_date DESC
            ) AS rn
            FROM forecast
        ),
        last_3_forecasts AS (
            SELECT
            the_coordinates,DATE(the_date) AS the_day,the_temp
        FROM ranked_forecasts
        WHERE rn <= 3
    )
    SELECT
        the_coordinates,
        the_day,
        AVG(the_temp) AS average_temp
    FROM last_3_forecasts
    GROUP BY the_coordinates, the_day;
        """  
        cursor.execute(query)
        results = cursor.fetchall()
        averages = []
        for row in results:
            averages.append({
                'Date': row[1],
                'Coordinates': row[0],
                'Average Temp': float(row[2])
            })
        return render_template('average_temp.html', averages=averages)
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/top_locations', methods=['GET'])
def top_locations():
    #Get the top n locations based on each available metric.
    try:
        # Get the 'n' parameter from the request query string
        n = int(request.args.get('n', 3))  # Default is 3 because we have 3 locations.

        connection = get_db_connection()
        cursor = connection.cursor()

        # For temperature
        query_temp = """
        WITH ranked_temp AS (
            SELECT 
                the_coordinates,
                MAX(the_temp) AS max_temp
            FROM forecast
            GROUP BY the_coordinates
            ORDER BY max_temp DESC
            LIMIT %s
        )
        SELECT * FROM ranked_temp;
        """
        cursor.execute(query_temp, (n,))
        top_temperature = cursor.fetchall()

        # For humidity
        query_humidity = """
        WITH ranked_humidity AS (
            SELECT 
                the_coordinates,
                MAX(the_humidity) AS max_humidity
            FROM forecast
            GROUP BY the_coordinates
            ORDER BY max_humidity DESC
            LIMIT %s
        )
        SELECT * FROM ranked_humidity;
        """
        cursor.execute(query_humidity, (n,))
        top_humidity = cursor.fetchall()

        # For wind speed
        query_windspeed = """
        WITH ranked_windspeed AS (
            SELECT 
                the_coordinates,
                MAX(the_windspeed) AS max_windspeed
            FROM forecast
            GROUP BY the_coordinates
            ORDER BY max_windspeed DESC
            LIMIT %s
        )
        SELECT * FROM ranked_windspeed;
        """
        cursor.execute(query_windspeed, (n,))
        top_windspeed = cursor.fetchall()

        return render_template('top_locations.html', 
                               top_temperature=top_temperature,
                               top_humidity=top_humidity,
                               top_windspeed=top_windspeed,
                               n=n)

    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == '__main__':
    app.run(debug=True)