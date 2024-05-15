"""
Program to receive the contents of a queue created in 'streaming-producer.py'

Author: Solomon Stevens
Date: May 17th, 2024

Basic Steps:
1. Connect to the queue
2. "Consume" each element of the queue

NOTE: Requires 'pika' to be installed beforehand

"""

# ===== Preliminary Stuff =====================================================

# Imports
import pika, sys, os

# Constants
HOST = 'localhost'
QUEUE_NAME = 'queue_key_name'

# ===== Main ==================================================================
def main():
    # Connect to the queue
    # -> Create a connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = HOST))

    # -> Create a channel
    channel = connection.channel()


    # ===== Functions =========================================================
    
    # --- Define the callback function ---
    def callback(ch, method, properties, body):
        """
        Function to receive the contents of the queue
        """
        print(f'Received: {body.decode()}')

    # ===== Call the Queue ====================================================

    # Grab next item from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback= callback, auto_ack=True)

    # Display beginning text
    print('Process Beginning: Press CTRL+C to end the stream')

    # Start consuming elements from the queue
    channel.start_consuming()


# ===== Run Main ==========================================================
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted: Stream Ended')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)