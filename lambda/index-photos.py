import json
import boto3
import requests

s3 = boto3.client('s3')

def get_info_from_S3(event):
	bucket = event['Records'][0]['s3']['bucket']['name']
	name = event['Records'][0]['s3']['object']['key']
	event_time = event['Records'][0]['eventTime']
	
	print(bucket)
	print(name)
	print(event_time)
	
	response = s3.head_object(Bucket=bucket, Key=name)

	if 'customlabels' in response['Metadata']:
		custom_labels = response['Metadata']['customlabels']
		label_arr = custom_labels.split(',')
	else:
		label_arr = []
	
	print("Custom labels:")
	print(label_arr)
	
	return bucket, name, event_time, label_arr


def detect_labels(photo, bucket):
	print("xxxxxxx")
	print(photo)
	print(bucket)
	
	client=boto3.client('rekognition')

	response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}}, MaxLabels=10)

	print('Detected labels for ' + photo) 
    
	res_labels = []
      
	for label in response['Labels']:
		print ("Label: " + label['Name'])
		res_labels.append(label['Name'])
		
	print(res_labels)
	return res_labels


def create_json(bucket, name, event_time, labels):
	
	json_dict = {
		"objectKey": name,
		"bucket": bucket,
		"createdTimestamp": event_time,
		"labels": labels
	}
	
	res = json.dumps(json_dict)
	print(res)
	
	return res


def index_to_elastic_search(payload):
	
	url = 'https://search-photos-lwjkxtwicijdh4tvclidnpcfya.us-east-1.es.amazonaws.com/photos/Photo/'
	headers = {"Content-Type": "application/json"}
	
	res = requests.post(url, auth=("Smart_photo123", "Smart_photo123"), data=payload.encode("utf-8"), headers=headers)
	
	print("Response of Post Request is: ")
	print(res.text)
	

def lambda_handler(event, context):
    
    bucket, name, event_time, custom_labels = get_info_from_S3(event)
    labels = detect_labels(name, bucket)
    
    final_labels = custom_labels + labels
    print(final_labels)
    
    ans = create_json(bucket, name, event_time, final_labels)
    
    index_to_elastic_search(ans)