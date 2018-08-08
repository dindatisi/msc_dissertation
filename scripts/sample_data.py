import pandas as pd
import random
import numpy as np
from operator import itemgetter
from collections import Counter
import datetime
from scipy import interp
import warnings
import merge_sample
import get_customers 
import get_what_you_need as gwyn

SOURCE_DF = '/project/samples/sample_first.pickle'
SOURCE_PATH = '/project/customers_df/final/all_sample/'

SOURCE_PATH_LISTENERS = '/project/customers_df/exp/fragments/listeners_historical/'
SOURCE_PATH_NONLISTENERS = '/project/customers_df/exp/fragments/non_listeners/'

    
def get_first_stream_source(df):
    # leave only first time track_id streaming per customer
    temp = df.drop_duplicates(subset=['customer_id','track_id'])
    temp['first_stream_source'] = temp['stream_source']
    df = df.merge(temp[['customer_id','track_id','first_stream_source']].reset_index(), on=['customer_id','track_id'],how='left')
    return df
    
def filter_df(df_2):
    print('filter data')
    df_2 = get_first_stream_source(df_2)
    df_2 = df_2[df_2.first_stream_source!='collection']
    return df_2
    
def concat_sample(df_2):
    #filter existing customers
    df_1 = pd.read_pickle(SOURCE_DF).sort_values(by='logtime')
    df_1 = filter_df(df_1)
    df_1 = df_1[~df_1.customer_id.isin(df_2.customer_id.unique().tolist())]
    df = pd.concat([df_1,df_2],ignore_index=True).sort_values(by='logtime')
    print('\ntotal rows:',len(df))
    print('unique customers:',df.customer_id.nunique())
    df.to_pickle('/project/samples/sample_added_more.pickle')
    print('sample saved')

def generate_additional_chunks():
    df_1 = pd.read_pickle(SOURCE_DF)
    existing = df_1.customer_id.unique().tolist()
    get_customers.get_month_chunk(existing=existing)
    
def get_additional_df():
    print('start merging additional')
    df_2 = merge_sample.get_merged_data(SOURCE_PATH).sort_values(by='logtime')
    df_2 = filter_df(df_2)
    concat_sample(df_2)
   

def merge_two_df():
    print('reading first file')
    df1 = pd.read_pickle('samples/sample_first3months.pickle')
    df1 = filter_df(df1)
    print('reading 2nd file')
    df2 = pd.read_pickle('/project/samples/sample_added_more.pickle')
    df2 = filter_df(df2)
    pd.concat([df1,df2],ignore_index=True).to_pickle('/project/samples/sample_first.pickle')
    print('file saved')

def magic():
    print('start merging files')
    #months_in=['201503','201504','201505']
    df = merge_sample.get_merged_data(SOURCE_PATH,min_week=12).sort_values(by='logtime')
    print('filtering')
    df = filter_df(df)
    print('getting what you need')
    gwyn.get_em_all(df,'/project/samples/new/sample_mixed_105k.pickle')
    
def main():
    #generate_additional_chunks()
    #get_additional_df()
    magic()

if __name__ == "__main__":
    main()