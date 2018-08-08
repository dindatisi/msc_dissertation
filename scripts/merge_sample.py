import pandas as pd
import os
import random

SOURCE_PATH = '/project/customers_df/poc/fragments/new/'
months_in = ['201503','201504','201505']

def get_file_lists(SOURCE_PATH):
    files = os.listdir(SOURCE_PATH)
    filepaths = [(SOURCE_PATH + name) for name in files if name[:6] in months_in]
    files_use = [f for f in filepaths if len(f)<81]
    print(len(files_use))
    return files_use

def get_combined_df(filepaths):
    df_list = []
    for path in filepaths:
        try:  
            print(path)
            df = pd.read_pickle(path)
            print(len(df))
            df_list.append(df)
        except:
            print(path + 'FAILED')      
    df = pd.concat(df_list,ignore_index=True)
    print('length combined:',len(df))
    return df

    
def preprocess_df(df):
    df = df.sort_values(by='date')
    df['date'] = pd.to_datetime(df.date, errors='coerce')
    df['month'] = df.date.dt.month
    df['week'] = df.date.dt.weekofyear
    df['day'] = df.date.dt.dayofyear
    df['logtime'] = pd.to_datetime(df.logtime, errors='coerce')
    return df
    
def get_total_months(df):
    tot = df.groupby('customer_id')['month'].nunique()
    return df.customer_id.map(tot)

def filter_users(df,mode='churn', start_month=3):
    if mode=='churn':
        user_list = df[(df.month==start_month) | (df.month==(start_month+1))].customer_id.tolist()
        df = df[df.customer_id.isin(user_list)]
    elif mode=='full months':
        df['total_months'] = get_total_months(df)
        df = df[df.total_months==2]
    # drop week if there's min week
    print('total rows:', len(df))
    print(df.groupby('month')['customer_id'].nunique())
    return df

def get_merged_data(source,min_week=None):
    files = os.listdir(source)
    print(len(files))
    filepaths = [(source + name) for name in files]
    print('first 5 files:',filepaths)
    df = get_combined_df(filepaths)
    df = preprocess_df(df)
    if min_week != None:
        print('filtering for min week:', min_week)
        df = df[df.week>=min_week]
    #df = filter_users(df)
    print('total rows:',len(df))
    return df
   
def main():
    filepaths = get_file_lists(SOURCE_PATH)
    df = get_combined_df(filepaths)
    print('\npreprocessing')
    df = preprocess_df(df)
    #print('filtering month')
    #df = filter_users(df)
    print('total rows:',len(df))
    df.to_pickle('samples/sample_34_150k.pickle')
    return df
    
if __name__ == "__main__":
    main()
    