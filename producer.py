import sched, time
import urllib2
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaConsumer

WEATHER_API_APPID = '156d4c2fa5186e1b7b49edbae5819c21'

# Get weather data from API for 4 cities every 5 mins and producer to Kafka topic: weather
scheduledJob = sched.scheduler(time.time, time.sleep)
def get_weather(sc): 

	# To send messages synchronously
	kafka = KafkaClient('localhost:9092')
	producer = SimpleProducer(kafka)

	countriesArray = ["Singapore", "Chicago", "Madrid", "Beijing"]

	for country in countriesArray:

		# Call Weather API to get forecasts
		response = urllib2.urlopen('http://api.openweathermap.org/data/2.5/weather?q=' + country + '&appid=' + WEATHER_API_APPID)
		data = json.load(response)   

		countryDataDict = {}
		countryDataDict["city"] = data["name"]
		countryDataDict["country"] = data["sys"]["country"]
		countryDataDict["timestamp"] = data["dt"]
		countryDataDict["wind_speed"] = data["wind"]["speed"]
		countryDataDict["visibility"] = data["visibility"]
		countryDataDict["weather"] = data["weather"]
		countryDataDict["main"] = data["main"]

		# Need to convert dict to bytes before sending to kafka
		bytesData = json.dumps(countryDataDict)

		producer.send_messages(b'weather', b'Weather data for ' + country + ' at ' + str(data["dt"]))
		producer.send_messages(b'weather', bytesData)
		print("Weather data for " + country + " sent to Kafka..")

	sc.enter(300, 1, get_weather, (sc,))

scheduledJob.enter(300, 1, get_weather, (scheduledJob,))
scheduledJob.run()