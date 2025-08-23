CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id INT NOT NULL,
    temperature NUMERIC(5,2) NOT NULL, 
    humidity NUMERIC(5,2) NOT NULL,     
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sensor_id ON sensor_data(sensor_id);
CREATE INDEX IF NOT EXISTS idx_timestamp ON sensor_data(timestamp);