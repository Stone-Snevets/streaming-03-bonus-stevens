"""
Program to read in contents from CSV file and send them to a queue

Author: Solomon Stevens
Date: May 17th, 2024

Basic Steps:
1. Read in the data from the CSV file
2. Establish a connection to a queue
3. Read the contents of each row
4. Send each row's contents to the queue

NOTE: Requires 'pika' to be installed beforehand.

"""
# ===== Preliminary Stuff =====================================================

# Imports
import csv, pika, time

# Constants
HOST = 'localhost'
QUEUE_NAME = 'queue_key_name'
DELAY_SEC = 3
INPUT_FILE = 'DRN_Nats.csv'
DELIMITER = '\t'


# ===== Functions =============================================================

# --- Define a function to read the contents of a row ---
def read_row(row):
    """
    Function to split the contents of a row into respective categories

    """
    # Read in the contents of the row
    round_num, question_num, point_value, question_type, answer_type, location_type, hit_point, notes = row

    # Create a format string of the row's contents
    f_str = f'[{round_num}, {question_num}, {point_value}, {question_type}, {answer_type}, {location_type}, {hit_point}, {notes}]'

    # Generate a binary message of our f string
    MSG = f_str.encode()

    # Return the binary message
    return MSG
    

# --- Define queue sender function ---
def send_to_queue(reader_obj):
    """
    Function to send each row of a reader object into a queue

    """

    # Establish a queue

    # -> Create a connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host = HOST))

    # -> Create a channel
    channel = connection.channel()

    # -> Declare the queue
    channel.queue_declare(queue = QUEUE_NAME)

    # For each row
    for row in reader_obj:
        # Check if row is empty
        if row != []:
            # If not, send row to queue
            channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body = read_row(row))
            print ('Sent:', row)

            # Set a timer for DELAY_SEC seconds
            time.sleep(DELAY_SEC)

    # Close the connection
    connection.close()



# --- Define file reader function ---
def csv_read(input_file_name):
    """
    Function to read in the contents of a CSV file

    """

    # Open the file
    with open(input_file_name, 'r') as input_csv:

        # Create a reader object
        reader = csv.reader(input_csv, delimiter=DELIMITER)

        # Skip the header row
        header = next(reader)

        # Send the reader object to send_to_queue function
        send_to_queue(reader)


# ===== Main ==================================================================
if __name__ == '__main__':
    csv_read(INPUT_FILE)