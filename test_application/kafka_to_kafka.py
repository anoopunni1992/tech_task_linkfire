#----Code Developed By : Anoop U
#----Date: 07-oct-2021
#----purpose: LinkFire Interview Task 
#----Code Logic: Sample working application which performs below logic.
#--------Python Module Implemented to map Json message from new schema ( Schema corresponding to new data pipeline)
#--------to old schema (Schema corresponding to old data pipeline). The Json message with old schema will be forwarded to 
#--------another kafka topic, applications relaying on old data pipeline can make use of this message.


##---Import required modules----##
from kafka import KafkaConsumer
from json import loads
from json import dumps
from kafka import KafkaProducer
import logging
import constants as cns
##---------------------------------##

#-----------Function to Instantiate required variables------------------------#
class kafka_2_kafka:
    
    def __init__(self):
        
        #Initialize Kafka consumer to consume data from new data pipeline
        self.consumer = KafkaConsumer(cns.TOPIC_1,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='grp1',
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        #Initialize Kafka producer to send old data schema generated from new schema to old data pipeline topic.
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        #Instantiate list's with all the keys requried for the old Json Schema#
        self.old_schema_keys = ['sessionToken', 'timestamp','timeuuid','event']
        self.old_schema_keys_1 = ['asset','pipeline','inventoryId']
        self.old_schema_keys_2 = ['artists']
        logging.basicConfig(filename="msg_details.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',level=logging.INFO)
        self.logger=logging.getLogger()

#-----------------------------------------------------------------------------#
 
#-------------Function to generate old data schema from new data schema-------#       
    def create_old_schema(self,message,old_schema,schema_keys) -> dict:
        for key in schema_keys: #iterate through old data schema keys
            if key in message:  # Check the whether the key is present in data with new schema
                old_schema[key] = message.get(key) #if key is present, populate key and corresponding value to old schema object.
                
            else:
                err_msg = f"Key: {key} is not found in session message['sessionToken'] at timestep message['timestamp']"
                print(err_msg)
                self.logger.info(err_msg)
                old_schema[key]=None #if key is not present, populate key and value as None
                
        return old_schema
#-----------------------------------------------------------------------------#     

#----------The Function passes new messages recieved to create_old_schema function,  
#----------and the converted data will be written to old pipeline topic.-----------#  
    def process_next(self):
        for message in self.consumer:
            data=message.value
        
            
            if data:
    
                old_schema = {} # Initialize a new dict object for every new record
                # Invoke create_old_schema() to generate old data schema from new schema
                old_schema=self.create_old_schema(data,old_schema,self.old_schema_keys)
                old_schema=self.create_old_schema(data['message']['assetMetadata'],old_schema,self.old_schema_keys_2)
                old_schema=self.create_old_schema(data['message'],old_schema,self.old_schema_keys_1)
            
                print(old_schema)
                #Write data with old schema to kafka topic corresponding to old pipeline
                try:
    
                    self.producer.send(cns.TOPIC_2, value=old_schema)
                except Exception as e:
      
                    self.logger.info(f"Exception while producing record value -{data} to topic {cns.TOPIC_2}: {e}")
       
            else:
                
                self.logger.info("Empty message found !!")
            
#---------------------------------------------------------------------------------------------#
            
obj = kafka_2_kafka()
obj.process_next()
        
        