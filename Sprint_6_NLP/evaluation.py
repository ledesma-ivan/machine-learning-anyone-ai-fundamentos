import pandas as pd
import matplotlib.pyplot as plt

from sklearn import metrics
from sklearn.preprocessing import label_binarize
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve
from sklearn.metrics import auc



def get_performance(predictions, y_test, labels=[1, 0]):
    # Put your code
    accuracy = metrics.accuracy_score(y_test, predictions)  # replace
    precision = metrics.precision_score(y_test, predictions)  # replace
    recall = metrics.recall_score(y_test, predictions)  # replace
    f1_score = metrics.f1_score(y_test, predictions)  # replace
    
    report = classification_report(y_test, predictions)  # replace
    
    cm = confusion_matrix(y_test, predictions)  # replace
    cm_as_dataframe = pd.DataFrame(data=cm)
    
    print('Model Performance metrics:')
    print('-'*30)
    print('Accuracy:', accuracy)
    print('Precision:', precision)
    print('Recall:', recall)
    print('F1 Score:', f1_score)
    print('\nModel Classification report:')
    print('-'*30)
    print(report)
    print('\nPrediction Confusion Matrix:')
    print('-'*30)
    print(cm_as_dataframe)
    
    return accuracy, precision, recall, f1_score


def plot_roc(model, y_test, features):
    # Put your code
    predic = model.predict_proba(features)
    pred_prob = predic[:, 1]
    fpr = metrics.roc_curve(y_test, pred_prob)[0]  # replace
    tpr = roc_curve(y_test, pred_prob)[1]  # replace
    roc_auc = metrics.auc(fpr,tpr)  # replace

    plt.figure(figsize=(10, 5))
    plt.plot(fpr, tpr, label=f'ROC curve (area = {roc_auc})', linewidth=2.5)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC) Curve')
    plt.legend(loc="lower right")
    plt.show()

    return roc_auc