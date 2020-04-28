# coding=utf-8



from sklearn.metrics import roc_curve, auc
import matplotlib.pyplot as plt
import pandas as pd

import random







def plot_roc_curve(actual,predictions):
    false_positive_rate, true_positive_rate, thresholds = roc_curve(actual, predictions)
    roc_auc = auc(false_positive_rate, true_positive_rate)
    plt.title('train Receiver Operating Characteristic')
    plt.plot(false_positive_rate, true_positive_rate, 'b',
    label='AUC = %0.2f'% roc_auc)
    plt.legend(loc='lower right')
    plt.plot([0,1],[0,1],'r--')
    plt.xlim([-0.1,1.0])
    plt.ylim([-0.1,1.0])
    plt.ylabel('True Positive Rate')
    plt.xlabel('False Positive Rate')
    plt.show()
    plt.savefig("C:\Users\Administrator\Desktop\liushi\data20190702\\roc\\rf\\1.png")


if __name__ == '__main__':
    predict=pd.read_csv('C:\Users\Administrator\Desktop\liushi\data20190702\\roc\\rf\\rf_x_train.csv')
    actual=pd.read_csv('C:\Users\Administrator\Desktop\liushi\data20190702\\roc\\rf\\y_train.csv')
    plot_roc_curve(actual=actual,predictions=predict)
