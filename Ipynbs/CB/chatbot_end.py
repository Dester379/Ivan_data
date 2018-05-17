import pandas as pd
import numpy as np
import re
import pymorphy2
from nltk.corpus import stopwords 
from sklearn.externals import joblib

gs = joblib.load('chatbot.pkl')
df1 = pd.read_excel('bots_answers.xlsx')

def tokenizer(s):
    morph = pymorphy2.MorphAnalyzer()   
    s = re.sub("[^а-яА-Я]", " ", s)    #only words ostavlyaem
    t = s.lower().split()              #маленькие буквы и разделяем
    f = []
    for j in t:
        m = morph.parse(j.replace('.',''))
        if len(m) != 0:
            wrd = m[0]
            if wrd.tag.POS not in ('NUMR','PREP','CONJ','PRCL','INTJ'):   #удаляем части речи плохие
                f.append(wrd.normal_form)
    garbagelist = ['спасибо', 'пожалуйста', 'добрый', "доброго", 'вечер', 'прошу', 'доброе', 'утро' , 'здравствуйте',
                   'здраствуйте', 'добпый', 'здрасте', "подскажите", "скажите", 'помогите','здравствовать','ваш', 'здраствовать']
    f = [word for word in f if word not in stopwords.words('russian')]   #удаляем стоп-слова
    f = [word for word in f if word not in garbagelist]   #удаляем 
    return ( " ".join(f))   

    
def generate_answer(x):
        if x.lower().find('как дела')!=-1 or x.lower().find('как твои дела') != -1 or x.lower().find('как ты') != -1:
            return "Прекрасно!"
        if x.lower()=='здраствуйте' or x.lower()=='привет' or x.lower()=='здравствуйте' or x.lower()=='здрасьте':
            return "Добрый день!"
        elif x.lower().find('сколько тебе лет') != -1:
            return "42"
        elif x.lower().find('как тебя зовут') != -1 or x.lower().find('кто ты') != -1 or x.lower().find('как зовут') != -1:
            return "Алина"
        elif x.lower().find('спасибо') != -1 or x.lower().find('благодарю') != -1 or x.lower().find('понял') != -1 or x.lower().find('поняла') != -1 or x.lower().find('понятно') != -1 or x.lower().find('ясно') != -1:
            raise ValueError("Обратитесь, пожалуйста, к оператору с Вашим вопросом")
        elif x.lower().find('оператор') != -1 or x.lower().find('оператором') != -1 or x.lower().find('оператору') != -1:
            raise ValueError("Обратитесь, пожалуйста, к оператору с Вашим вопросом")
        elif len(x.split(' '))<3:
            raise ValueError("Обратитесь, пожалуйста, к оператору с Вашим вопросом")    
        
        x = tokenizer(x)
        if (np.max(gs.predict_proba([x])) ) > 0.7:
            return df1['ans'][df1.freq == (gs.predict([x]))[0]].any(),df1['tag'][df1.freq == (gs.predict([x]))[0]].any()
        else:
            raise ValueError("Обратитесь, пожалуйста, к оператору с Вашим вопросом")
        
var = input("Добрый день! Задайте, пожалуйста Ваш вопрос: ")

if __name__ == "__main__": 
    print (generate_answer(var))