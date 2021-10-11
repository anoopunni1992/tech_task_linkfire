# Linkfire Data Engineer Technical Task

## Solution Details
Solution for the task (both code and theoretical explanation) is kept under (https://github.com/anoopunni1992/tech_task_linkfire/tree/main/tech_task_solution)

## Test Application
A working application, just to represent the working of schema mapping logic is kept under (https://github.com/anoopunni1992/tech_task_linkfire/tree/main/test_application)
The test application consists of a kafka producer, which sends data with new schema to “new_topic”. Logic to consume the data 
from new_topic, schema mapping logic and pushing the mapped data to old_topic is written in the module kafka_to_kafka.py
