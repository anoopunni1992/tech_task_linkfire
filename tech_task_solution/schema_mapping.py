#----Code Developed By : Anoop U
#----Date: 07-oct-2021
#----purpose: LinkFire Interview Task 
#----Code Logic:
#--------Python Module Implemented to convert Json message from new schema ( Schema corresponding to new data pipeline)
#--------to old schema (Schema corresponding to old data pipeline). The Json message with old schema will be forwarded to 
#--------another kafka topic, applications relaying on old data pipeline can make use of this message.
   


##---Import required modules----##
import logging
import time
import structlog
# Local
from . import base # base
from ..connections import kafka
##---------------------------------##


class Kafka2Kafka(base.Sink):

#-----------Function to Instantiate required variables------------------------#   
    def __init__(self, logger: logging.Logger) -> None:
        
        logger.debug("Instantiating Kafka Reader")
        self.reader = kafka.Reader(config=config, logger=logger.getChild("kafka_reader"))
        logger.debug("Instantiating Kafka Writer")
        self.writer = kafka.Writer(config, logger.getChild("writer"))
        self.logger = logger
        
        #----Instantiate list's holding all the keys requried for the old Json Schema ---#
        self.old_schema_keys = ['sessionToken', 'timestamp','timeuuid','event']
        self.old_schema_keys_1 = ['asset','pipeline','inventoryId']
        self.old_schema_keys_2 = ['artists']

#-----------------------------------------------------------------------------#

#-------------Function to generate old data schema from new data schema-------#
        
    def create_old_schema(self,message,old_schema,schema_keys) -> dict:
        for key in schema_keys: #iterate through old data schema keys
            if key in message:  # Check the whether the key is present in data with new schema 
                old_schema[key] = message.get(key) #if key is present, populate key and corresponding value to old schema object.
                
            else:
                err_msg = f"Key: {key} is not found in session message['sessionToken'] at timestep message['timestamp']"
                self.logger.debug(err_msg)
                old_schema[key]=None #if key is not present, populate the key and corresponding value as None
                
        return old_schema
#-----------------------------------------------------------------------------#               
                    
                                
#----------The Function passes new messages recieved to create_old_schema function,  
#----------and the converted data will be written to old pipeline topic.-----------#
    def process_next(self):
        #"""Atomically process the next record in line."""#
        
        data = self.reader.read() # assuming the return type of read() as Python Dictionary
        old_schema = {} # Initialize a new dict object for every new record
        
        # Invoke create_old_schema() to generate old data schema from new schema
        old_schema=self.create_old_schema(data,old_schema,self.old_schema_keys)
        old_schema=self.create_old_schema(data['message']['assetMetadata'],old_schema,self.old_schema_keys_2)
        old_schema=self.create_old_schema(data['message'],old_schema,self.old_schema_keys_1)
                        
        #Write data with old schema to kafka topic corresponding to old pipeline
        self.writer.write(old_schema)
   
#---------------------------------------------------------------------------------------------#
         
    def close(self) -> None:
        self.writer.close()
        
if __name__ == "__main__":
    Kafka2Kafka.run()
        
        



 
    