import pandas as pd 
import os
import numpy as np
from sklearn.utils import resample
import random

SOURCE_PATH = '/project/pickles/'
DESTINATION_PATH = '/project/customers_df/final/all_sample/'
MONTHS = ['201503','201504','201505']
TRACK_ID = 'f72fa60c8d9848a393d8ac4bbaa866ef'


# for exp dataset
DESTINATION_LISTENERS = '/project/customers_df/exp/fragments/listeners_historical/'
DESTINATION_NONLISTENERS = '/project/customers_df/exp/fragments/non_listeners/'

# for final dataset
#DESTINATION_LISTENERS = '/project/customers_df/final/listeners/'
#DESTINATION_NONLISTENERS = '/project/customers_df/final/non_listeners/'
TRACK_CUSTOMERS = '/project/customers_df/final/listeners/track_listeners.csv'
NONTRACK_CUSTOMERS = '/project/customers_df/final/non_listeners/non_track_listeners.csv'
DATASET2_CUST = '/project/customers_df/final/dataset2_customers.csv'


def read_data(filepath):
    df = pd.read_pickle(filepath)
    return df

def get_total_customers():
    filenames = os.listdir(SOURCE_PATH)
    print('first 5 files:',filenames[:5])
    total = []
    for name in filenames:
        filepath = SOURCE_PATH + name
        # get unique customer_id in filepath
        print("Working on", name)
        num = get_nunique_customers(filepath)
        print('nunique: ',num)
        total.append(num)
    return sum(total)

def get_nunique_customers(filepath):
    df = pd.read_pickle(filepath)
    return df.customer_id.nunique()

               
def process_sample(df, id_list):
    df = df.drop(['track_name'],axis=1) 
    df = df[df.customer_id.isin(id_list)]
    return df
    
def get_sample(filepath,id_list):
    df = pd.read_pickle(filepath)
    df = process_sample(df, id_list)
    return df

def find_artist(filepath, artist_name):
    df = pd.read_pickle(filepath)
    df = df[df.artist_name==artist_name]
    return df
    
def loop_files(files_use, artist_name):
    df = pd.DataFrame()
    for name in files_use:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        df = find_artist(filepath,artist_name)
        if len(df)>0:
            print(df)
        else:
            print('artist not found')
    print('done')

def filter_customers(df):
    '''filer missing attributes
    and free-mobile & basic-desktop customers
    so we have customers with complete information
    '''
    # without birth year & gender
    df = df.dropna(subset=['birth_year','gender'], how='any')
    # basic-desktop & deleted users
    excl = df[(df.access=='basic-desktop') | (df.access=='deleted')].customer_id.unique()
    df = df[~df.customer_id.isin(excl)]
    # free mobile users
    excl = df[(df.access=='free') & (df.stream_device=='mobile')].customer_id.unique()
    df = df[~df.customer_id.isin(excl)]
    print('total customers after filtering: ',df.customer_id.nunique())
    return df

def get_track_df(filepath, TRACK_ID=TRACK_ID):
    df = pd.read_pickle(filepath)
    users = df[df.track_id == TRACK_ID].customer_id
    df = df[df.customer_id.isin(users)]
    df = filter_customers(df)
    
    #get only the row we need to know
    df = df[df.track_id == TRACK_ID].sort_values(by='logtime').drop_duplicates(subset='customer_id',keep='first')
    print('total customers:',len(df))
    return df

    
def get_track_customers(filepath, TRACK_ID=TRACK_ID,n_sample=300000):
    df = pd.read_pickle(filepath)
    users = df[df.track_id == TRACK_ID].customer_id
    df = df[df.customer_id.isin(users)]
    df = filter_customers(df)
    # sample 
    users = df.customer_id.unique().tolist()
    # handling
    if n_sample>len(users):
        n_sample = len(users)
    print('available sample:',n_sample)
    sample_id = random.sample(users,n_sample)
    df = df[df.customer_id.isin(sample_id)]
    return df

def get_nontrack_customers(filepath, TRACK_ID=TRACK_ID, n_sample=300000):
    df = pd.read_pickle(filepath)
    users = df[df.track_id == TRACK_ID].customer_id.unique()
    df = df[~df.customer_id.isin(users)]
    df = filter_customers(df)  
    # sample 
    users = df.customer_id.unique().tolist()
    # handling
    if n_sample>len(users):
        n_sample = len(users)
    sample_id = random.sample(users,n_sample)
    df = df[df.customer_id.isin(sample_id)]
    return df

def main_track():
    filenames = os.listdir(SOURCE_PATH)
    print(filenames)
    files_use = [f for f in filenames if f[:6] in MONTHS]
    for name in files_use:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        df = get_track_customers(filepath)
        output_name = DESTINATION_LISTENERS + name
        df.to_pickle(output_name)
        

def get_track_non_duplicates():
    filenames = os.listdir(SOURCE_PATH)
    print(filenames)
    files_use = [f for f in filenames if f[:6] in MONTHS]
    for name in files_use:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        df = get_track_df(filepath, TRACK_ID)
        output_name = DESTINATION_LISTENERS + name
        df.to_pickle(output_name)
    

def main_nontrack():
    filenames = os.listdir(SOURCE_PATH)
    files_use = [f for f in filenames if f[:6] in MONTHS]
    for name in files_use:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        df = get_nontrack_customers(filepath)
        output_name = DESTINATION_NONLISTENERS  + name
        df.to_pickle(output_name)

def main():
    filenames = os.listdir(SOURCE_PATH)
    file_saved = os.listdir('/project/customers_df/poc/fragments/')
    files_use = [f for f in filenames if f[:6] in MONTHS]
    all_df = []
    c_list = pd.read_csv(TRACK_CUSTOMERS,squeeze=True).tolist()
    for name in files_use:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        df = get_sample(filepath,c_list)
        output_name = DESTINATION_NONLISTENERS + name
        df.to_pickle(output_name)
        
def get_from_clist(output_destination,list_file):
    filenames = os.listdir(SOURCE_PATH)
    files_use = [f for f in filenames if f[:6] in MONTHS]
    all_df = []
    c_list = pd.read_csv(list_file,squeeze=True).tolist()
    for name in files_use:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        df = get_sample(filepath,c_list)
        output_name = output_destination + name
        df.to_pickle(output_name)
    
if __name__ == "__main__":
    main()


    