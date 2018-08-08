'''
getting ready for experiments in jupyter notebook
df in SOURCE_PATH must be cleaned data
'''

import pandas as pd 


# CONSTANT
SOURCE_PATH = '/project/samples/sample_34_100k_processed.pickle' #cleaned and balanced sample
OUTPUT_NAME= '/project/exp_output/time_series_data.csv'
TRACK_ID = 'f72fa60c8d9848a393d8ac4bbaa866ef'


def get_customer_in_sample(df, df_track1):
    ''' 
    get streaming data of all customers who listen to the track 
    '''
    stream_df = df[df.customer_id.isin(df_track1.customer_id.unique())]
    print(len(stream_df))
    print(stream_df.customer_id.nunique())
    return stream_df
    
def get_groups(df_track1,stream_df):
    '''return bucket containing playlist and album
        where each item has been filtered 
        to contain only playlists & albums on df_track1
    '''
    playlist = stream_df[stream_df.playlist_id.isin(df_track1[df_track1.first_stream_source=='others_playlist'].playlist_id.unique())]
    playlist = playlist[playlist.customer_id.isin(df_track1[df_track1.first_stream_source=='others_playlist'].customer_id.unique())]
    playlist = get_avg_stream_count(playlist,mode='week') 
    album = stream_df[stream_df.album_name.isin(df_track1[df_track1.first_stream_source=='album'].album_name.unique())]
    album = album[album.customer_id.isin(df_track1[df_track1.first_stream_source=='album'].customer_id.unique())]
    album = get_avg_stream_count(album,mode='week') 
    print(playlist.customer_id.nunique(), album.customer_id.nunique())
    return [playlist,album]
    

def get_week_arrive(data):
    # get week customer first listened to the track based on date from df_track
    arrive = data.groupby('customer_id')['week'].min().reset_index() 
    arrive = arrive.rename(columns={'week':'first_week'})
    data = data.merge(arrive,on=['customer_id'], how='left')
    return data

def get_avg_stream_count(df,mode='week'):
    # either for before of after, with day and week as mode
    # input either post or pre df
    count = df.groupby(['customer_id',mode])['index'].count().reset_index()
    col_name = str(mode+'ly_stream_count')
    count.rename(columns={'index':col_name},inplace=True)
    df = df.merge(count, on=['customer_id',mode], how='left')
    return df

def get_flagged_bucket(bucket):
    if len(bucket)>1:
        bucket[0]['group'] = 'playlist'
        bucket[1]['group'] = 'album'
    else:
        pass
    return bucket

def split_pre_post_bucket(df_track,bucket):
    '''
    split pre & post track group for item in bucket
    set the flag, return two buckets
    '''
    bucket_prior = []
    # get first time customer listened to the track
    mintime_customer = df_track.groupby('customer_id')['logtime'].min().reset_index()
    mintime_customer = mintime_customer.rename(columns={'logtime':'min_time'})
    for i,dataframe in enumerate(bucket):
        dataframe = dataframe.merge(mintime_customer,on=['customer_id'],how='left')
        dataframe_after = dataframe[dataframe.logtime>=dataframe.min_time]
        dataframe_after['is_post'] = 1
        dataframe_before = dataframe[dataframe.logtime<=dataframe.min_time]
        dataframe_before['is_post'] = 0
        dataframe_before = dataframe_before[dataframe_before.track_id!=TRACK_ID]
        bucket[i] = dataframe_after
        bucket_prior.append(dataframe_before)
    return bucket_prior,bucket
    
def split_pre_post_nonlisteners(df,week=16):
    '''
    split pre & post track based on week chosen
    '''
    dataframe_after = dataframe[dataframe.week>week]
    dataframe_after['is_post'] = 1
    dataframe_before = dataframe[dataframe.logtime<=week]
    dataframe_before['is_post'] = 0
    return dataframe_before,dataframe_after
    

def concat_group(bucket):
    '''concat playlist & album into one df_track
        can take either pre or post
    '''
    bucket = get_flagged_bucket(bucket)
    merged = pd.concat(bucket,ignore_index=True).sort_values(by='logtime')
    return merged

def set_observation_period(df_track,bucket,period=7,mode='day'):
    '''
    bucket whatever, takes both pre and post
    return merged df... uh this is a badly designed program
    '''
    merged = concat_group(bucket)
    df_track['next_period'] = df_track[mode].astype('int') + period
    df_track['prev_period'] = df_track[mode].astype('int') - period
    next_Nday = df_track.groupby('customer_id')['next_period'].min().to_dict()
    prev_Nday = df_track.groupby('customer_id')['prev_period'].min().to_dict()
    merged['cut_time_lower'] = merged['customer_id'].map(prev_Nday)
    merged['cut_time_upper'] = merged['customer_id'].map(next_Nday)
    return merged
    
def set_observation_period_cg(df,period=7,mode='day'):
    '''
    for control goup (nonlisteners)
    '''
    df['next_period'] = df[mode].astype('int') + period
    df['prev_period'] = df[mode].astype('int') - period
    next_Nday = df.groupby('customer_id')['next_period'].min().to_dict()
    prev_Nday = df.groupby('customer_id')['prev_period'].min().to_dict()
    df['cut_time_lower'] = df['customer_id'].map(prev_Nday)
    df['cut_time_upper'] = df['customer_id'].map(next_Nday)
    return df


def get_prior_stats(merged_pre,mode='day'):
    # last N day (period)
    # to append to df_exp
    cutoff = merged_pre.copy()
    cutoff = cutoff[cutoff[mode]>=merged_pre.cut_time_lower]
    num_songs = cutoff.groupby('customer_id')['track_id'].nunique().to_dict()
    num_artists = cutoff.groupby('customer_id')['artist_name'].nunique().to_dict()
    num_albums = cutoff.groupby('customer_id')['album_name'].nunique().to_dict()
    num_playlists = cutoff.groupby('customer_id')['playlist_id'].nunique().to_dict()
    prior_stream_count = cutoff.groupby('customer_id')['customer_id'].count().to_dict()
    merged_pre['num_songs'] = merged_pre['customer_id'].map(num_songs)
    merged_pre['num_artists'] = merged_pre['customer_id'].map(num_artists)
    merged_pre['num_albums'] = merged_pre['customer_id'].map(num_albums)
    merged_pre['num_playlists'] = merged_pre['customer_id'].map(num_playlists)
    merged_pre['prior_stream_count'] = merged_pre['customer_id'].map(prior_stream_count)
    return merged_pre

def get_post_stats(merged_post,mode='day'):
    cutoff = merged_post.copy()
    cutoff = cutoff[cutoff[mode]<=cutoff.cut_time_upper]
    post_stream_count = cutoff.groupby('customer_id')['customer_id'].count().to_dict()
    merged_post['post_stream_count'] = merged_post['customer_id'].map(post_stream_count)
    return merged_post

def filter_cohort(data, n=2):
    return data[data.first_week==n]

def get_pre_post_df(df_track,bucket):
    # split bucket into pre and post
    bucket_pre, bucket_post = split_pre_post_bucket(df_track,bucket)
    # for each, set the observation period
    df_pre = set_observation_period(df_track,bucket_pre,period=7)
    df_post = set_observation_period(df_track,bucket_post,period=7)
    # get stats
    df_pre = get_prior_stats(df_pre)
    df_post = get_post_stats(df_post)
    return df_pre,df_post
    
def get_pre_post_non_listeners_df(df):
    # split bucket into pre and post
    df_pre,df_post = split_pre_post_nonlisteners(df,week=16)
    # for each, set the observation period
    df_pre = set_observation_period_cg(df,period=7,mode='day')
    df_post = set_observation_period_cg(df,period=7,mode='day')
    # get stats
    df_pre = get_prior_stats(df_pre)
    df_post = get_post_stats(df_post)
    return df_pre,df_post
   
def time_series_analysis(df_track,bucket):
    # bucket is group in main
    df_pre,df_post = get_pre_post_df(df_track,bucket)
    df_exp = pd.concat([df_pre,df_post],ignore_index=True).drop_duplicates(subset=['customer_id','week'])
    output = '/project/exp_output/exp.csv'
    df_exp.to_csv(output,index=False)
    t_playlist = df_exp[(df_exp.first_stream_source=='others_playlist')].groupby('week')['weekly_stream_count'].mean()
    t_album = df_exp[(df_exp.first_stream_source=='album')].groupby('week')['weekly_stream_count'].mean()
    df_t = pd.concat([t_album,t_playlist],axis=1)
    df_t.columns = ['album','playlist']
    df_t = df_t.reset_index()
    df_t['diff'] = df_t.playlist - df_t.album
    df_t.to_csv(OUTPUT_NAME,index=False)
    return df_t
    
def main():
    print('reading')
    df = pd.read_pickle(SOURCE_PATH)
    print('filtering')
    df_track1 = df[df.track_id==TRACK_ID]
    stream_df = get_customer_in_sample(df, df_track1)
    df_track1 = get_week_arrive(df_track1)
    df_track1 = filter_cohort(df_track1, n=15)
    print('identifying groups')
    groups = get_groups(df_track1,stream_df)
    print('getting time-series data')
    df_t = time_series_analysis(df_track1,groups)
    print('saved and done')



if __name__ == "__main__":
    main()
    

    