'''
This is the final final processing code.
Input: full-length proper sample pickle file.
Output: what you need to do analysis :)
'''

import pandas as pd
import random
import numpy as np


#  --------------- GLOBAL CONSTANT --------------- #
SOURCE_FILE = '/project/samples/sample_added_more.pickle'
OUTPUT_NAME = '/project/samples/sample_34_processed.pickle'
#  --------------- --------------- --------------- #

# Playlist info generator
def get_playlist_id(df):
    ss_df = df.stream_source_uri.str.split(pat=":",expand=True)
    return ss_df[[2,4]]
    
# previous stream source 
def get_date_to_collection(df):
    collections = df[df.stream_source=='collection']
    date_added = collections.groupby(['customer_id','track_id'])['logtime'].min()
    df = df.merge(date_added.reset_index(), on=['customer_id','track_id'],how='left')
    #df['added_to_collection'] = df[['customer_id','track_id']].map(date_added)
    df.rename(columns = {'logtime_y':'added_to_collection','logtime_x':'logtime'}, inplace = True)
    return df
    
def get_collection_source(df):
    # remove non-collection stream
    df = get_date_to_collection(df)
    data = df.dropna(subset=['added_to_collection'])
    prev_df = data[data.logtime<data.added_to_collection].sort_values(by='logtime')
    # locate latest stream before song is added to collection
    prev_df = prev_df.drop_duplicates(['customer_id','track_id'],keep='last')
    prev_source = prev_df.groupby(['customer_id','track_id'])['stream_source'].unique()
    df = df.merge(prev_source.reset_index(), on=['customer_id','track_id'],how='left')
    df['stream_source_y'] = df['stream_source_y'].fillna('')
    df['collection_source'] = df['stream_source_y'].apply(get_str_in_array)
    df = df.drop(['stream_source_y'],axis=1)
    df.rename(columns = {'stream_source_x':'stream_source'}, inplace = True)
    return df

def get_prev_source(df):
    # only consider track that has >1 num_source
    # otherwise
    df = get_num_source(df)
    temp = df.drop_duplicates(subset=['customer_id','track_id','stream_source'])
    temp = temp[temp.num_source>1]
    prev_source = temp.groupby(['customer_id','track_id'])['stream_source'].unique()
    df = df.merge(prev_source.reset_index(), on=['customer_id','track_id'],how='left')
    df['stream_source_y'] = df['stream_source_y'].fillna('')
    df['previous_source'] = df['stream_source_y'].apply(get_str_in_array)
    df = df.drop(['stream_source_y'],axis=1)
    df.rename(columns = {'stream_source_x':'stream_source'}, inplace = True)
    return df
    
def get_first_source_dummy(df):
    ss = pd.get_dummies(df.first_stream_source)
    df_new = pd.concat([df, ss], axis=1)
    return df_new
    
def get_str_in_array(x):
    if len(x)<1:
        return ''
    else:
        return str(x[0])
        

# track source info
def get_repeated_stream_count(df):
    # count of repeated track streaming
    repeated = df.groupby(['customer_id','track_id'])['logtime'].count()
    df = df.merge(repeated.reset_index(), on=['customer_id','track_id'],how='left')
    df.rename(columns = {'logtime_x':'logtime', 'logtime_y':'track_repeat_count'}, inplace = True)
    return df

def get_num_source(df):
    # number of stream source of the track per customer
    repeated = df.groupby(['customer_id','track_id'])['stream_source'].nunique()
    df = df.merge(repeated.reset_index(), on=['customer_id','track_id'],how='left')
    df.rename(columns = {'stream_source_x':'stream_source', 'stream_source_y':'num_source'}, inplace = True)
    return df
    
# user stats
def get_num_songs(df):
    songs = df.groupby('customer_id')['track_id'].nunique().to_dict()
    df['num_songs'] = df.customer_id.map(songs)
    return df

def get_num_albums(df):
    album = df.groupby('customer_id')['album_name'].nunique().to_dict()
    df['num_albums'] = df.customer_id.map(album)
    return df

def get_num_playlists(df):
    p = df.groupby('customer_id')['playlist_id'].nunique().to_dict()
    df['num_playlists'] = df.customer_id.map(p)
    return df

def get_num_artists(df):
    artists = df.groupby('customer_id')['artist_name'].nunique().to_dict()
    df['num_artists'] = df.customer_id.map(artists)
    return df

def get_user_streams(df):
    sc = df.groupby('customer_id')['customer_id'].count().to_dict()
    df['user_stream_count'] = df.customer_id.map(sc)
    return df

def get_age_bins(data):
    data['birth_year']=data['birth_year'].dropna()
    data['age'] = 2015 - data.birth_year
    data['age_bin'] = pd.cut(data['age'], [0, 11, 17, 29, 49, 120], labels=['dependent', 'teens', 
                                                                              'young adult', 'adult','senior'])
    data = pd.get_dummies(data,columns=['age_bin'],drop_first=True)
    data = data.drop(['birth_year'],axis=1)
    return data
    
def get_listening_stats(df):
    df = get_num_songs(df)
    df = get_num_albums(df)
    df = get_num_playlists(df)
    df = get_num_artists(df)
    df = get_user_streams(df)
    return df

# aggregate
def get_em_all(df,OUTPUT_NAME):
    print('1')
    df = get_collection_source(df)
    print('2')
    df[['playlist_creator','playlist_id']] = get_playlist_id(df)
    print('3')
    df = get_age_bins(df)
    print('4')
    df = get_num_source(df)
    print('5')
    df = get_repeated_stream_count(df)
    print('6 gonna be a long wait')
    df = get_listening_stats(df)
    print('7 saving')
    df.to_pickle(OUTPUT_NAME)
    print('file saved')

def main():
    print('reading file')
    df = pd.read_pickle(SOURCE_FILE).sort_values(by='logtime')
    df = df.reset_index()
    df = df.drop(['level_0','index'],axis=1)
    get_em_all(df,OUTPUT_NAME)

if __name__ == "__main__":
    main()