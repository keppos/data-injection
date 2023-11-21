# number_producer.py
from kafka import KafkaProducer
import json

def send_numbers():
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # List of unsorted numbers
    unsorted_numbers = [5, 3, 8, 1, 2]

    # Send the unsorted numbers to the 'unsortedNumbers' topic
    producer.send('unsortedNumbers', value=unsorted_numbers)
    
    # Close the producer
    producer.close()

if __name__ == "__main__":
    # Run the send_numbers function when the script is executed
    send_numbers()
