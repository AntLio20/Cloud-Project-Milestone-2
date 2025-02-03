from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob
import json
import os
import random
import numpy as np  # pip install numpy
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("No JSON key file found. Ensure your service account key is in the working directory.")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
print("Using service account:", files[0])  # DEBUG

# Set the project_id with your project ID
project_id = "ID"
topic_name = "NAME"  # Change if needed

# Create a publisher and get the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

# Device normal distributions profile used to generate random data
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091)},
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341)},
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201)},
}

profileNames = list(DEVICE_PROFILES.keys())

# Initialize a unique ID
ID = np.random.randint(0, 10000000)

while True:
    profile_name = random.choice(profileNames)
    profile = DEVICE_PROFILES[profile_name]

    # Get random values within a normal distribution
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))

    # Create dictionary
    msg = {
        "ID": ID,
        "time": int(time.time()),
        "profile_name": profile_name,
        "temperature": temp if random.random() >= 0.1 else None,  # 10% chance to be None
        "humidity": humd if random.random() >= 0.1 else None,  # 10% chance to be None
        "pressure": pres if random.random() >= 0.1 else None  # 10% chance to be None
    }
    ID += 1

    # Serialize message as JSON and publish
    try:
        record_value = json.dumps(msg).encode('utf-8')  # Ensure proper encoding
        future = publisher.publish(topic_path, record_value)
        future.result()  # Ensure the message is published
        print(f"Published message: {msg}")
    except Exception as e:
        print(f"Failed to publish message: {str(e)}")  # Print any errors

    time.sleep(0.5)  # Wait for 0.5 second
