import pika
import pyodbc
import json
import datetime


def on_message(channel, method_frame, header_frame, body):
    # print(method_frame.delivery_tag)
    # print(body)
    x = json.loads(body)
    message_time = datetime.datetime.fromtimestamp(x["timestamp"])
    # print(message_time)
    #print(f'received a {x["priority"]["description"]} message')
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    command = f"INSERT into Message (MessageTime, ReceivedTime, Text, Priority)  VALUES ('{message_time}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{x['message']}', {x['priority']['level']});"
    print(command)
    cursor = connectionPyodbc.cursor()
    cursor.execute(command)
    connectionPyodbc.commit()

    cursor = connectionPyodbc.cursor()
    cursor.execute ("SELECT * from Message;") 
    row = cursor.fetchone() 
    while row: 
        print(row[0])
        row = cursor.fetchone()



credentials = pika.PlainCredentials('espada', '@espada')
parameters = pika.ConnectionParameters(host='localhost', port='5672', credentials=credentials, blocked_connection_timeout=300)
connectionRabbitmq = pika.BlockingConnection(parameters)

server = 'mssql' 
database = 'Test' 
username = 'espada' 
password = '@espada' 
driver = 'ODBC Driver 18 for SQL Server'

connectionPyodbc = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes')
print("Conexão bem sucedida")







channel = connectionRabbitmq.channel()
channel.basic_consume('message', on_message)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connectionRabbitmq.close()

