import requests
from bs4 import BeautifulSoup
import uuid
import boto3

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2024"
    response = requests.get(url)
    
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }
        
    array = []
    dic = response.json()
    arrreturn = []
    arr = []

    for iterator in dic:
        array.append((iterator["createdAt"], iterator))
    
    array.sort(key=lambda x: x[0], reverse=True)
    
    for iterator in range(100):
        arr.append(array[iterator])
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingPropuesto')
    
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})
    
    iterator = 1
    for row in arr:
        data = row[1]
        data['#'] = iterator
        data['id'] = str(uuid.uuid4())
        arrreturn.append(data)
        table.put_item(Item=data)
        iterator += 1
    
    return {
        'statusCode': 200,
        'body': arrreturn
    }
