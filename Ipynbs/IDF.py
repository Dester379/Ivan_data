import pymysql.cursors
import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
import scikitplot as skplt
import eli5
from IPython.display import display
#from sklearn.model_selection  import train_test_split
from sklearn.metrics import roc_auc_score, roc_curve, log_loss, f1_score, confusion_matrix, precision_score, recall_score, classification_report, accuracy_score
scaler = StandardScaler()
label = LabelEncoder()

def import_all_modules():
    import scikitplot as skplt
    from sklearn.decomposition import TruncatedSVD
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import StandardScaler
    from sklearn.preprocessing import LabelEncoder
    import matplotlib.pyplot as plt
    from pandas import ExcelWriter
    import scipy
    scaler = StandardScaler()
    label = LabelEncoder()
    #sql
    import pymysql.cursors 
    #NLP
    import nltk
    import re
    import pymorphy2
    from nltk.tokenize import sent_tokenize, RegexpTokenizer
    from nltk.stem.snowball import RussianStemmer
    from nltk.util import ngrams
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from difflib import SequenceMatcher
    #warnings
    import warnings
    warnings.filterwarnings('ignore')
    #visualisation
    import seaborn as sns
    import matplotlib.pyplot as plt
    sns.set(style="white", color_codes=True)        
    #vectorization
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.feature_extraction.text import TfidfTransformer
    #model 
    from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
    from sklearn.cluster import KMeans
    from sklearn.cross_validation import cross_val_score
    from sklearn.cross_validation import train_test_split
    from sklearn.cross_validation import KFold
    from sklearn.metrics import roc_auc_score, roc_curve, log_loss, f1_score, confusion_matrix, precision_score, recall_score, classification_report, accuracy_score
    #classificators
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.ensemble import ExtraTreesClassifier
    from sklearn.linear_model import SGDClassifier
    from sklearn.ensemble import VotingClassifier
    from sklearn.ensemble import GradientBoostingClassifier
    from xgboost.sklearn import XGBClassifier 
    from sklearn.svm import SVC
    import datetime
    from sklearn.externals import joblib


def del_id(df):
    for i in df.columns:
        if i.find('_id')!=-1:
            df.drop(i, axis=1, inplace=True)
            
def find_col(df, word):
    for i in df.columns:
        for j in range(len(df)):
            try:
                if df[i][j].find(word) != -1:
                    print('Column: %s \n Row: %s' % (i, j))
            except:
                print ('There is no such word in dataframe')
    
def read_from_mysql(country, user, password, script):
    if country=='br':
        conn = pymysql.connect(host='192.168.64.1', port=3306,user=user,password=password, db='mysql')
    elif country=='es':
        conn = pymysql.connect(host='10.100.0.100', port=33062, user=user, password=password,db='mysql')
    elif country=='mx':
        conn = pymysql.connect(host='192.168.65.1', port=3306, user=user, password=password,db='mysql')
    elif country=='ge':
        conn = pymysql.connect(host='192.168.250.14', port=3306, user=user, password=password,db='mysql')
    elif country=='kz':
        conn = pymysql.connect(host='192.168.250.15', port=3306, user=user, password=password,db='mysql')
    elif country=='ru':
        conn = pymysql.connect(host='109.234.153.116', port=3306, user=user, password=password,db='mysql')
    elif country=='solva_ru':
        conn = pymysql.connect(host='192.168.250.50', port=3306, user=user, password=password,db='mysql')
    elif country=='solva_kz':
        conn = pymysql.connect(host='192.168.250.17', port=3306, user=user, password=password,db='mysql')
    elif country=='solva_ge':
        conn = pymysql.connect(host='192.168.250.13', port=3306, user=user, password=password,db='mysql')
    elif country=='amp_ru':
        conn = pymysql.connect(host='95.213.187.6', port=3306, user=user, password=password,db='mysql')
    else:
        print ('unknown country')
    return pd.read_sql(script, con=conn)

def preprocessing(df, cols_to_drop, y_col=None, is_need_scale=False, cols_to_byte=None):
    # function for bytes columns    
    for i in cols_to_drop:
        try:
            df.drop(i, axis=1, inplace=True)
        except:
            pass
    def bytes_to_string(x):
        try:
            return str(ord(x))
        except:
            return x
    def del_id(df):
        for i in df.columns:
            if i.find('_id')!=-1:
                df.drop(i, axis=1, inplace=True)
    # replace bytes columns
    if cols_to_byte:
        try: 
            for col in cols_to_byte:
                df[col] = df[col].map(lambda x: bytes_to_string(x))
        except:
            print ('Bytes problems')
    # Fillna
    df.fillna(-1, inplace=True)
    # Encoding
    cat_val = df.select_dtypes(include=[object]).columns
    try:
        for i in cat_val:
            #df[i]=df[i].map(lambda x: str(x))
            #df[i]=label.fit_transform(df[i])
            df[i]=label.fit_transform(df[i].astype(str))
    except:
        print ('Label problems')
    # Split and Target
    if y_col:
        y = df[y_col]
        X = df.drop(y_col, axis=1)
        if is_need_scale:
            try:                
                X = scaler.fit_transform(X)
            except:
                print ('Scaler problems')
        return X, y
    else:
        return df
    

def plot_score(clf, X_test, y_test, feat_to_show=30, is_normalize=False, cut_off=0.5):
    cm = confusion_matrix(pd.Series(clf.predict_proba(X_test)[:,1]).apply(lambda x: 1 if x>cut_off else 0), y_test)
    print ('ROC_AUC:  ', roc_auc_score(pd.Series(clf.predict_proba(X_test)[:,1]).apply(lambda x: 1 if x>cut_off else 0), y_test))
    print ('Gini:     ', 2*roc_auc_score(pd.Series(clf.predict_proba(X_test)[:,1]).apply(lambda x: 1 if x>cut_off else 0), y_test) - 1)
    print ('F1_score: ', f1_score(pd.Series(clf.predict_proba(X_test)[:,1]).apply(lambda x: 1 if x>cut_off else 0), y_test))
    print ('Log_loss: ', log_loss(clf.predict(X_test), y_test))
    
    print ('\n')
    print ('Classification_report: \n', classification_report(pd.Series(clf.predict_proba(X_test)[:,1]).apply(lambda x: 1 if x>cut_off else 0), y_test))
    skplt.metrics.plot_confusion_matrix(y_test, pd.Series(clf.predict_proba(X_test)[:,1]).apply(lambda x: 1 if x>cut_off else 0), title="Confusion Matrix",
                    normalize=is_normalize,figsize=(8,8),text_fontsize='large')
    #print ('\n')
    imp = pd.DataFrame(list(zip(X_test.columns, clf.feature_importances_)))
    imp = imp.reindex(imp[1].abs().sort_values().index).set_index(0)
    imp = imp[-feat_to_show:]
    #график_фич
    ax = imp.plot.barh(width = .6, legend = "", figsize = (12, 10))
    ax.set_title("Feature Importances", y = 1.03, fontsize = 16.)
    _ = ax.set(frame_on = False, xlabel = "", xticklabels = "", ylabel = "")
    for i, labl in enumerate(list(imp.index)):
        score = imp.loc[labl][1]
        ax.annotate('%.2f' % score, (score + (-.12 if score < 0 else .02), i - .2), fontsize = 10.5)
    try:
        display(eli5.show_weights(clf, top=20, feature_names = list(X_test.columns)))
    except:
        pass
    
def similar_words(a, b):
    q = []
    for word1 in a.split(' '):
        ma = 0
        for word2 in b.split(' '):            
            if SequenceMatcher(None, word1, word2).ratio()>ma:
                ma = SequenceMatcher(None, word1, word2).ratio()        
        q.append(ma)    
    return np.mean(q)

def similar_sentences(a, b):
    q = []
    for i in range(len(a)):
        q.append(similar(str(a[i]), str(b[i])))
    return np.mean(q)

def ren_cols(df):
    for i in range(len(df.columns)):
        for j in range(len(df.columns)):
            if df.iloc[:,i].name==df.iloc[:,j].name and i!=j:
                df.columns.values[i] = df.columns.values[i]+'_1'
                df.columns.values[j] = df.columns.values[j]+'_2'
