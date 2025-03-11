import os

def walkdir(folder):
    for path, _, files in os.walk(folder):
        for filename in files:
            yield(path, filename)


def get(path):
    X=[]
    y=[]
    for review in ["pos","neg"]:
        for dirpath, filename in walkdir(os.path.join(path, review)):  
            label = review 
            with open (os.path.join(dirpath, filename)) as f:
                X.append(f.read())
                if label == "pos":
                    y.append(1)
                else:
                    y.append(0)
    return X,y