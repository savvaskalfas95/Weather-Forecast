import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# List of location in coordinates
locations_cordinates = ['36.3992,25.4793','35.3397,25.1803','36.7167,24.45'] #Athina,Heraklion,Milos

def fetch_weather_data(api_url):
        #credentials provided by free trial
        User = 'none_kalfas_savvas'
        Password = '44Bv5OxIxz'
        response = requests.get(api_url, auth=(User, Password)) 
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

def store_data_in_mysql(data, db_config):

    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        if connection.is_connected():
            cursor = connection.cursor()
            # Get the latitude and longitude
            latitude = data['data'][0]['coordinates'][0]['lat']
            longitude = data['data'][0]['coordinates'][0]['lon']
            coordinates = f"{latitude},{longitude}"

            # Extracting arrays from the JSON structure
            temperature_array = data['data'][0]['coordinates'][0]['dates']
            humidity_array = data['data'][1]['coordinates'][0]['dates']
            wind_speed_array = data['data'][2]['coordinates'][0]['dates']

            # Looping through the array size
            for item in temperature_array:
                # Convert date format
                datetime_obj = datetime.strptime(item['date'], "%Y-%m-%dT%H:%M:%SZ")
                formatted_date = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        
                # Extract relevant values
                temperature_value = item['value']
                humidity_value = next((humidity['value'] for humidity in humidity_array if humidity['date'] == item['date']), None)
                wind_speed_value = next((wind['value'] for wind in wind_speed_array if wind['date'] == item['date']), None)

                # Insert into the database
                cursor.execute(
                    "INSERT INTO forecast (the_date, the_temp, the_coordinates, the_humidity, the_windspeed) VALUES (%s, %s, %s, %s, %s)",
                    (formatted_date, temperature_value, coordinates, humidity_value, wind_speed_value)
                )
                connection.commit()
            print("Data inserted successfully.")

    except Error as e:
        print(f"Error: {e}")
        if connection.is_connected():
            connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")


def main():
    #api_url_template, dynamicaly change the {location} placeholder with cordinates
    api_url_template='https://api.meteomatics.com/2024-06-17T00:00:00Z--2024-06-23T23:00:00Z:PT4H/t_2m:C,relative_humidity_2m:p,wind_speed_850hPa:mph/{location}/json'

    #database config
    db_config = {
        'user': 'root',  
        'password': 'test1', 
        'database': 'weatherAPI'  
    }

    try:
        #loop through the cordinates to fetch data from the API, the location parameter is mandatory and must be in cordinates
        for cordinates in locations_cordinates:
            api_url=api_url_template.replace("{location}",cordinates)
            api_data = fetch_weather_data(api_url)
            store_data_in_mysql(api_data, db_config)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()