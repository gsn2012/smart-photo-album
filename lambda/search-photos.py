import json
import boto3
import requests


def searchElastic(keys):
	photos = []
    
	for k in keys:
		print(type(k))
		url = "https://search-photos-lwjkxtwicijdh4tvclidnpcfya.us-east-1.es.amazonaws.com/_search"
		query = {
			"size": 1000,
			"query": {
				"match": {
					"labels":k
				}
			}
		}
	
		headers = { "Content-Type": "application/json" }
		r = requests.get(url, auth=("Smart_photo123","Smart_photo123"), headers=headers, data = json.dumps(query)).json()
		
		print("Elastic Response: ")
		print(json.dumps(r))
		
		for item in r["hits"]["hits"]:
			
			bucket = item["_source"]["bucket"]
			name = item["_source"]["objectKey"]
			print(bucket)
			print(name)

			photoURL = "https://s3.amazonaws.com/{0}/{1}".format(bucket, name)
			print(photoURL)
			photos.append(photoURL)
        
	return photos


def clean_words(res):
	query = []
    
	if res["slots"]["first_keyword"] != None:
		query.append(res["slots"]["first_keyword"])
		print(res['slots']['first_keyword'])
	if res["slots"]["second_keyword"] != None:
		query.append(res["slots"]["second_keyword"])
		print(res['slots']['second_keyword'])
            
	return query

def sendToLex(message):
	client = boto3.client('lex-runtime')
	lex_response = client.post_text(
		botName ='KeywordExtractor',
		botAlias = 'KeywordExtractor',
		userId = "user_id",
		inputText = message
	)
    
	print(lex_response)
	return lex_response

def lambda_handler(event, context):
	photos = []
    
    
	message = event['queryStringParameters']['q']
	print("msg:")
	print(message)
	lex_response = sendToLex(message)
	keys = clean_words(lex_response)
	photos = searchElastic(keys)
    

    
	return {
		'statusCode': 200,
		'body': json.dumps(photos),
		'headers': {
			'Access-Control-Allow-Headers' : 'Content-Type',
			'Access-Control-Allow-Origin': '*',
			'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
		}
	}