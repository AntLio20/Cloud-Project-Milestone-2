from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # for searching for JSON file
import json
import os
import csv

#Search for the JSON service account key
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
else:
    print("No service account JSON found!")
    exit()

#Set the project_id with your project ID
project_id = "ID"
topic_name = "TOPIC"

#Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

#Path to the CSV file
file_path = "Labels.csv"

def convert_value(value):
    """Convert CSV values to int, float, or keep as string."""
    try:
        if "." in value: 
            return float(value)
        return int(value) 
    except ValueError:
        return value

with open(file_path, mode='r') as csv_file:
    reader = csv.DictReader(csv_file)

    for row in reader:
        # Convert each value dynamically
        converted_row = {key: convert_value(value) for key, value in row.items()}

        # Serialize the message correctly
        message = json.dumps(converted_row).encode('utf-8')

        #send the value
        print("Publishing record:", message)
        # Publish the message
        future = publisher.publish(topic_path, message)

        # Ensure publishing is successful
        future.result()

print("All records have been published.")
