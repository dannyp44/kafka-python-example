from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import multiprocessing
import os
import yaml
import threading
from yaml import Loader, SafeLoader
from dotenv import load_dotenv
from retrying import retry
from exceptions import *
from thirdparty_processing import process


stop_event = multiprocessing.Event()

# Custom constructor to handle environment variable substitution
def env_var_constructor(loader, node):
    value = loader.construct_scalar(node)
    return os.path.expandvars(value)


with open('application.yaml', 'r') as stream:
      # Load environment variables from .env file
    load_dotenv()
    # Add the custom constructor to the SafeLoader
    yaml.add_constructor('!ENV_VAR', env_var_constructor, Loader=SafeLoader)    
    config = yaml.safe_load(stream)
   
    # Read retry configuration
    retry_config = config.get('retry', {})
    max_attempts = retry_config.get('maxAttempts', 3)
    delay = retry_config.get('maxDelay', 1000)  
   
    # Read kafka configuration
    kafkaGeneral = config['kafkaGeneral']
    bootstrap_servers = kafkaGeneral['defaultBootstrapAddress']
    defaultGroupId = kafkaGeneral['defaultGroupId']
    consumer = kafkaGeneral['consumer'] 
    topic = consumer['topic']
    autoCommit = consumer['autoCommit']
    pollRecords = consumer['pollRecords']
    pollIntervalMs = consumer['pollIntervalMs']
    consumersConcurrency = consumer['consumersConcurrency']


# Decorate retry to be able to print retry count
def retry_enrich_decorator():
    retry_count = [0]  # Using a list to create a mutable variable

    @retry(wait_fixed=delay, stop_max_attempt_number=max_attempts, retry_on_exception=lambda e: isinstance(e, ResourceUnavailableException))
    def wrapped_operation(data):
        retry_count[0] += 1
        try:
            return process(data)
        except Exception as e:
            if isinstance(e, ResourceUnavailableException):
                print(f"Exception caught on attempt {retry_count[0]}: {e}")
            raise  # Re-raise the exception

    return wrapped_operation

# Usage
retry_enrich_operation = retry_enrich_decorator()    


def worker(thread_id):
    consumer = KafkaConsumer(topic, 
                             bootstrap_servers=bootstrap_servers, 
                             group_id=defaultGroupId,
                             max_poll_records=pollRecords,
                             max_poll_interval_ms=pollIntervalMs,
                             enable_auto_commit=autoCommit)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer.subscribe([topic])    

    while not stop_event.is_set():    
        for message in consumer:
            if stop_event.is_set():
                break

            # Extract the message value
            message_value = message.value.decode('utf-8')
            print(f"Thread {thread_id} received message: {message_value}")

            # Convert the string to a JSON object
            try:
                json_object = json.loads(message_value)
                print(f"Thread {thread_id} received JSON object: {json_object}")
            except json.JSONDecodeError as e:
                print(f"Thread {thread_id} Error decoding JSON: {e}")
                continue  # Skip to the next iteration if decoding fails

            # Check if nesessary fields exist
            if 'data' in json_object and 'context' in json_object and 'replyTo' in json_object:
                data = json_object['data']
                reply_to = json_object['replyTo']
                context = json_object['context']

                # Do enrichment
                try:
                    enrichment = retry_enrich_operation(data)
                except Exception as e:
                    print(f"Thread {thread_id} Operation failed after retries: {e}")
                    enrichment = {}
                    result = "FAILED"
                else:
                    print(f"Thread {thread_id} Operation succeeded. Result: {enrichment}")
                    result = "PASSED"

                metadata = {"_result_": result}

                response = {
                    "metadata": metadata,
                    "data": enrichment,
                    "context": context
                }

                producer.send(topic=reply_to, value=response)
            else:
                print(f"Thread {thread_id} Missing one or more fields in the JSON object")

            # Manually commit the offset after processing the message
            consumer.commit()
        
    consumer.close()
    producer.close()



def main():

    print("Skeleton is starting...")    

    # Create X threads
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(consumersConcurrency)]

    # Start the threads
    [thread.start() for thread in threads]

    # Wait for all threads to finish
    [thread.join() for thread in threads]    


    # producer = KafkaProducer(
    #     bootstrap_servers=bootstrap_servers,
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    # consumer.subscribe([topic])
    # while not stop_event.is_set():    
    #     for message in consumer:

    #         # Extract the message value
    #         message_value = message.value.decode('utf-8')  
    #         print(message_value)

    #         # Convert the string to a JSON object
    #         try:
    #             json_object = json.loads(message_value)
    #             print(f"Received JSON object: {json_object}")
    #         except json.JSONDecodeError as e:
    #             print(f"Error decoding JSON: {e}")

    #         # Check if nesessary fields exist
    #         if 'data' in json_object and 'context' in json_object and 'replyTo' in json_object:                
    #             data = json_object['data']
    #             replyTo = json_object['replyTo']
    #             context = json_object['context']               

    #             # Do enrichment
    #             try:
    #                 enrichment = retry_enrich_operation(data)
    #             except Exception as e:                   
    #                 print(f"Operation failed after retries: {e}")  
    #                 enrichment = {}                 
    #                 result = "FAILED"                   
    #             else:                    
    #                 print(f"Operation succeeded. Result: {enrichment}")
    #                 result = "PASSED"

    #             metadata = {"_result_": result}                    

    #             response = {
    #                 "metadata": metadata,
    #                 "data": enrichment,
    #                 "context": context
    #             }               

    #             producer.send(topic=replyTo, value=response)

    #         else:
    #             print("Missing one or more fields in the JSON object")           
    

    #         if stop_event.is_set():
    #             break
    # consumer.close()
    # producer.close()

if __name__ == '__main__':
    main()