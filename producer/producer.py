from kafka import KafkaProducer
import json

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('TEST-IN', {"data": {"name": "John Doe","age": 30,"city": "Example City"},"replyTo": "TEST-OUT", "context": {"timestamp": "2023-01-01T12:00:00","itemId": "12345"}})
    producer.close()

if __name__ == '__main__':
    main()