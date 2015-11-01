
from flask import Flask, jsonify
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaConsumer
from kafka.consumer import SimpleConsumer

app = Flask(__name__)

WEATHER_API_APPID = '156d4c2fa5186e1b7b49edbae5819c21'

@app.route("/consume")
def consume():

	client = KafkaClient("localhost:9092")
	consumer = SimpleConsumer(client, "test-group", "weather")
	
	for message in consumer:
		print(message)

	return 'Message reading..'

if __name__ == "__main__":
	app.run()