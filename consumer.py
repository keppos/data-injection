# number_consumer.py
from kafka import KafkaConsumer
import json

def receive_numbers():
    # Create a Kafka consumer
    consumer = KafkaConsumer('unsortedNumbers',
                             group_id='my_group',
                             bootstrap_servers='localhost:9092',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    # Consume messages from the 'unsortedNumbers' topic
    for message in consumer:
        # Extract the unsorted numbers from the Kafka message
        unsorted_numbers = message.value
        print(f"Received unsorted numbers: {unsorted_numbers}")

        # Filter out non-numerical characters from the unsorted list
        numerical_elements = [element for element in unsorted_numbers if isinstance(element, (int, float))]

        # Sort the numerical elements
        sorted_numbers = sorted(numerical_elements)
        print(f"Sorted numbers: {sorted_numbers}")

if __name__ == "__main__":
    # Run the receive_numbers function when the script is executed
    receive_numbers()

