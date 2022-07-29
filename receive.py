import pika
import pyodbc
import json
import datetime

credentials = pika.PlainCredentials('espada', '@espada')
parameters = pika.ConnectionParameters(
    host='localhost', port='5672', credentials=credentials, blocked_connection_timeout=300)
connection = pika.BlockingConnection(parameters)


def on_message(channel, method_frame, header_frame, body):
    # print(method_frame.delivery_tag)
    # print(body)
    x = json.loads(body)
    message_time = datetime.datetime.fromtimestamp(x["timestamp"])
    # print(message_time)
    #print(f'received a {x["priority"]["description"]} message')
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    print(f"INSERT into Message (MessageTime, ReceivedTime, Text, Priority)  VALUES ('{message_time}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{x['message']}', {x['priority']['level']});")


channel = connection.channel()
channel.basic_consume('message', on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
