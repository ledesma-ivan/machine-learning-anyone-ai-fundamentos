
import joblib
import numpy as np
import pandas as pd
import redis
import json
import time
import operator

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.preprocessing.sequence import pad_sequences

db =redis.Redis(
    host = 'redis',
    port = 6379,
    db = 0
)


df = pd.read_csv('DataNew.csv',index_col=0)
"""
DF = DF.drop(columns=['Text','category'])

Y_train_columns = DF.columns

def modelFuction(text):
    
    

    m = joblib.load('ModeloSVC0.plk')

    tfidf_vectorizer = joblib.load('tfidf.plk')
    
    text_vectorizer = tfidf_vectorizer.transform([text])
    
    prediction = m.predict(text_vectorizer)
    
    re = []
   
    for pre in np.where(prediction==1)[1]:
    
        re.append(Y_train_columns[pre]) 

    return re """

def modelFuction(text):

        tokenizer = joblib.load('tokenizer.plk')

        sequence = tokenizer.texts_to_sequences([text])

        sequence = pad_sequences(sequence,maxlen=57)

        model = keras.models.load_model('model-conv1d.h5')

        prediction = model.predict(sequence)
        
        predScores = [score for pred in prediction for score in pred]

        predDict = {}

        for cla,score in zip(df.columns[1:],predScores):
            
            predDict[cla] = score
        
        preSort = sorted(predDict.items(), key=operator.itemgetter(1),reverse=True)[:3]


        result = []
        for i in preSort:
            
            if i[1] > 0.80:
                
                result.append(i[0])

        return result



def classify_process():

    while True:

        id,job_data = db.brpop('job')

        tex = json.loads(job_data)

        p = modelFuction(tex['text'])

        dict = {
            'predictions':p
        }

        res = json.dumps(dict)

        db.set(tex['id'],res)

        time.sleep(0.05)

if __name__ == "__main__":
  
    classify_process()

