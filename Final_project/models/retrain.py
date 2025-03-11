import mysql.connector
import joblib
from datetime import datetime
import pandas as pd
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from sklearn.model_selection import train_test_split
from keras.layers import Activation, Conv1D
from keras.models import load_model
from keras.models import Sequential
from keras.layers import Dense, Embedding, GlobalMaxPool1D, Dropout
from sklearn.metrics import f1_score,recall_score,precision_score
from tensorflow.keras.optimizers import Adam
from keras.callbacks import ReduceLROnPlateau, ModelCheckpoint
import mysql.connector



def tranform_binary(predictions):

    a = predictions.tolist()

    for i in range(len(a)):

        for j in range(len(a[i])):

            if a[i][j] >= 0.5:

                a[i][j] = 1

            else:

                a[i][j] = 0

    a = np.array(a)

    return a

def get_standar_metrics(predictions, y_test):
  
  a = tranform_binary(predictions)

  return f1_score(y_test, a, average="micro"),recall_score(y_test, a, average="micro"),precision_score(y_test, a, average="micro")   


def retrainModel():
    # Create conection
    db1 = mysql.connector.connect(host='db',user='root',password='ivan',port=3306,database='proyect') 

    mydb = db1.cursor()

    # Create query
    query = ("select * from PROYECT")

    # Execute query
    mydb.execute(query)

    myresult = mydb.fetchall()


    # Create dataframe with results
    df = pd.DataFrame(myresult)

    # Change name of columns
    df.columns = ['Text','category']



    NameColumns = ['cat0' , 'cat1' , 'cat2', 'cat3', 'cat4']

    cat = df['category'].str.split(',', expand=True)

    cat.columns = NameColumns

    cat = cat[cat.columns[0:3]]

    cat = cat.replace('',None)


    for i in cat:

        row = 0

        # Iter element
        for category in cat[i]:

            if category != 0:
            
                # Add 1 in column 
                cat.loc[row,category] = 1

            row += 1

    cat = cat.fillna(0)

    df = pd.concat([df,cat],axis=1)

    df = df.drop_duplicates()

    X = df.Text
    Y = df[df.columns[5:]]



    #num_words : the maximum number of words to keep, based on word frequency (for the 45232 products information), #oov : out-of-vocabulary token, it is agregated to words which are not in the 10000 most frequent ones
    tokenizer = Tokenizer(num_words=10000, oov_token="<OOV>")

    # encoding words of information to integers, Updates internal vocabulary based on a list of sequences.
    tokenizer.fit_on_texts(X) 

    word_index = tokenizer.word_index

    sequences = tokenizer.texts_to_sequences(X)

    # This takes the maxlen parameter as a referene and makes all the lists of the same lenght adding zeros on the left spaces, if nothins is pass ot will take the longest sequence as a reference
    padded = pad_sequences(sequences, padding="post") 

    # Save tokenizer
    joblib.dump(tokenizer,'tokenizer.plk')


    X_train, X_test, y_train, y_test = train_test_split(padded,Y, 
                                                        test_size=0.3, 
                                                        random_state=0)


    # Number of class
    num_classes = y_train.shape[1]

    # Total words
    max_words = len(word_index) + 1

    # Max words in a text
    maxlen = len(padded[0])

    optimizer = tf.keras.optimizers.Adam()

    model = Sequential()
    model.add(Embedding(max_words, 300, input_length=maxlen))
    model.add(Dropout(0.21))
    model.add(Conv1D(300, 3, padding='valid', activation='relu', strides=1))
    model.add(GlobalMaxPool1D()) # This layer creates a convolution kernel that is convolved with the layer input over a single spatial (or temporal) dimension to produce a tensor of outputs.
    model.add(Dropout(0.21))
    model.add(Dense(num_classes))
    model.add(Activation('sigmoid'))

    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['Precision','Recall', tf.keras.metrics.AUC()])

    callbacks = [
        ReduceLROnPlateau(),
        ModelCheckpoint(filepath='model-conv1d.h5', save_best_only=True)
    ]

    model.summary()
    

    model.fit(X_train, y_train,
                    epochs=30,
                    batch_size=32,
                    validation_split=0.3,
                    callbacks=callbacks)

    
    # Create predictions
    predictions = model.predict(X_test, batch_size=32)

    # Create metrics (f1,recall,preciosion)
    metrics = get_standar_metrics(predictions, y_test)

    # Create var for send to MYSQL
    var = (str(round(metrics[0],2)),datetime.today())

    db1 = mysql.connector.connect(host='db',user='root',password='ivan',port=3306,database='proyect') 

    mydb = db1.cursor()

    query = """ INSERT INTO proyect.retrain 
            (f1,Date)
            VALUES (%s,%s) """
    mydb.execute(query,var)

    db1.commit()

    return 'Successfully retrained model'





