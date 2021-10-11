#----Code Developed By : Anoop U
#----Date: 07-oct-2021
#----purpose: LinkFire Interview Task 
#----Code Logic: Kafka producer to mimic the new data Pipeline source


##---Import required modules----##
from time import sleep
from json import dumps
from kafka import KafkaProducer
import constants as cns
##---------------------------------##

def publish_data():
    
#-------Initialize Kafka producer, which mimics the new data pipeline source----#
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
#-------------------------------------------------------------------------------#
 
#---------sample data with schema corresponding to new data pipeline----------------#   
    json_data= {"sessionToken": "b3cb81750c9689e4f7a2c85413b72c37","timestamp": "2020-06-23T10:38:25.843591Z","timeuuid": "ab3824b4-b53d-11ea-88af-0242ac120003","event": "visit","schema": "visit-schema-1234","message":{"asset": "button","assetversion": "1.0.0","pipeline": "Waterslide","assetMetadata": {"artists": ["Ariana Grande"]},"inventoryId": "efbbd33a-197c-45af-a0dc-9d85e012ce99"}}
#-------------------------------------------------------------------------------------#   

#--------push data to new data pipeline Kafka Topic-------------------------------------#
    for i in range(1000):
        try:
            producer.send(cns.TOPIC_1, value=json_data)
            sleep(5)
        except Exception as e:
      
            print(f"Exception while producing record value -{json_data} to topic {cns.TOPIC_1}: {e}")
        else:
            print(f"Successfully produced value {json_data} to topic {cns.TOPIC_1}")
#---------------------------------------------------------------------------------------#
    
if __name__=="__main__":
    publish_data()   
