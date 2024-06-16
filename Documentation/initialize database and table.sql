create database weatherapi;
use weatherapi;

CREATE TABLE  forecast (
    id INT AUTO_INCREMENT PRIMARY KEY,
    the_coordinates VARCHAR(255) NOT NULL,
    the_temp DOUBLE NOT NULL,
    the_date DATETIME NOT NULL,
    the_humidity DOUBLE NOT NULL,
    the_windspeed DOUBLE NOT NULL
);