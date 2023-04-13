import pandas as pd
import time

if __name__=='__main__':
    star = time.time()
    path2 = r"C:\Users\drubianm\Desktop\workspace\moninte_phituango-development\python_scripts\_Python.csv"
    path = r"C:\Users\drubianm\Downloads\ml-latest-small\ratings.csv"
    df = pd.read_csv(path, sep=",")
    print(df.head())
    #c = df["rating"].groupby(df.iloc[:,2]).count()
    #print(c)

    fin = time.time()
    print(fin-star)

