
import redis
import json
import uuid
import time

# Create conection with redis
db =redis.Redis(
    host = 'redis',
    port = 6379,
    db = 0
    )

# Create fuction 
def model_predict(text):

    '''
    Receives an text and queues the job into Redis.
    Will loop until getting the answer from our ML service.

    '''

    # Create ID unique for text
    id = str(uuid.uuid4())

    # Create dictionary
    job_data = {
        'id' : id,
        'text' : text
    }

    # Transfor dictionary
    msj_str = json.dumps(job_data)

    #----- Add to queue -----#
    job_name = 'job'

    db.rpush(
            job_name,
            msj_str
        )
    #-------------------------#

    # Loop until we received the response from our ML model
    while True:
  
        # If exist response
        if db.get(id):
            
            # Add response to variable
            output_ = json.loads(db.get(id))

            # Delete id from queue
            db.delete(id)
           
            break
    
    time.sleep(0.02)

    return output_['predictions']