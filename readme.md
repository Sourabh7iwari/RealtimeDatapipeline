# install venv for virtual environment (recommended)
pip install venv

# create virtual environment & activate it
python3 -m venv .venv
source .venv/bin/activate

# install dependencie
pip install -r requirements.txt

# give permission to setup script
chmod +x run.sh

# run the setup script
./run.sh

## now setup is complete and our spark job is listening for incoming sensor data to feed to postgres sensordb 
# run the data generation py script in another terminal and make sure to activate the .venv there also
python3 sensor_data_generator.py
