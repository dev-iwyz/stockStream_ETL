import requests
#from twython import TwythonStreamer
from dotenv import dotenv_values
from time import sleep
from kafka import KafkaProducer
from json import dumps

#Get config variables
config = dotenv_values(".env")

host = config["host"]
rapidAPI=config["rapidAPI"]
api_key=config["api_key"]
api_secret=config["api_secret"]
access_token=config["access_token"]
access_secret_tok=config["access_secret_tok"]

#API variable
header= {
	    'x-rapidapi-host': "twelve-data1.p.rapidapi.com",
	    'x-rapidapi-key': rapidAPI
	    }
url="https://twelve-data1.p.rapidapi.com/"

#Initialize Producer
producer = KafkaProducer(bootstrap_servers=host )

#API call class
class api_request:
	def __init__(self, query, endpoint):
		
	    
		self.header = header
		self.url = url
		self.query = query
		self.endpoint=endpoint
		
	
	def response (self):
		response = requests.request("GET", self.url+self.endpoint, headers=self.header, params=self.query)
		return response.json()
		
querys ={
 "EMA":{"interval":"5min","symbol":"BTC/USD","time_period":"9","outputsize":"1","format":"json","series_type":"close"},
"RSI": {"symbol":"BTC/USD","interval":"5min","outputsize":"1","series_type":"close","time_period":"14","format":"json"},
"TS":{"interval":"5min","symbol":"BTC/USD","format":"json","outputsize":"1"}

}
endpoints = {"ts":"time_series","ema":"ema", "rsi":"rsi"}

#Twitter class here

#Loop through for every 1 minute

while True:

	#Code
	time_series = api_request(query = querys["TS"],endpoint=endpoints["ts"])
	date=time_series.response()["values"][0]["datetime"]
	close_price = round(float(time_series.response()["values"][0]["close"]),2)
	open_price = round(float(time_series.response()["values"][0]["open"]),2)
	#volume = time_series.response()["values"][0]["volume"]

	ema_api = api_request(query = querys["EMA"],endpoint=endpoints["ema"])
	ema_value = round(float(ema_api.response()["values"][0]["ema"]),2)

	rsi_api = api_request(query = querys["RSI"],endpoint=endpoints["rsi"])
	rsi_value = round(float(rsi_api.response()["values"][0]["rsi"]),2)

	#dic_send = {"date":date,"open":open_price,  "close":close_price, "ema":ema, "rsi":rsi}
	#values have length of 1
	#print (dic_send)
	print(date)
	print(ema_value)
	print(rsi_value)
	dic_send = {"date":date, "close":close_price, "open":open_price,  "ema":ema_value, "rsi":rsi_value}
	vString = f"{date},{close_price},{open_price},{ema_value},{rsi_value}"
	producer.send("stockTopic", value= bytes(vString, encoding='utf8'))
	print (vString)
	print(dic_send)
	sleep(5*60)

