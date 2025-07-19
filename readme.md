
# create virtual environment & activate it
```bash
python3 -m venv .venv &&
source .venv/bin/activate
```

# install dependencie
```bash
pip install -r requirements.txt
```

# give permission to setup script
```bash
chmod +x run.sh
```


# run the setup script
```bash
./run.sh
```


## now setup is complete and our spark job is listening for incoming sensor data to feed to postgres sensordb 
# run the data generation py script in another terminal and make sure to activate the .venv there also
```bash
python3 sensor_data_generator.py
```

# checking data, quering and pre analysis 
open pgadmin gui at `localhost:8080`

connect to postgres server
for credentials check docker-compose.yml
and query the database