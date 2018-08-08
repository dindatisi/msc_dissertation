'''
This script will read the whole streaming files
and calculate population statistics
'''

import pandas as pd 
import os
import numpy as np

SOURCE_PATH = '/project/raw_data/'
DESTINATION_PATH = '/project/uniques/'

def put_headers(df):
    col_names = ['track_id','artist_name',
    'album_name','customer_id','stream_source_uri']
    df.columns = col_names
    return df

def get_playlist_id(df):
    ss_df = df.stream_source_uri.str.split(pat=":",expand=True)
    return ss_df[4]

def read_data_chunk(filepath,chunksize):
    chunks = pd.read_table(filepath,header=None, compression='gzip',
                  usecols=[3,6,8,9,25],
                  sep='\x01',chunksize=chunksize,encoding='latin-1',error_bad_lines=False)
    
    print(chunks.get_chunk().head())
    
    return chunks
    
def get_file_data(filepath,chunksize=25000000):
    print('\nreading data')
    reader = read_data_chunk(filepath,chunksize)
    file_artists = set()
    file_albums = set()
    file_tracks = set()
    file_customers = set()
    file_playlists = set()
    file_rows = []
    
    for i,chunk in enumerate(reader):
        print('preprocess')
        df = put_headers(chunk)
        df['playlist_id'] = get_playlist_id(df)
        
        artists = df['artist_name'].str.lower().unique().tolist()
        albums = df['album_name'].str.lower().unique().tolist()
        tracks = df['track_id'].unique().tolist()
        customers = df['customer_id'].unique().tolist()
        playlists = df['playlist_id'].unique().tolist()
        rows = len(df)
        
        print('updating sets')
        file_artists.update(artists)
        file_albums.update(albums)
        file_tracks.update(tracks)
        file_customers.update(customers)
        file_playlists.update(playlists)
        file_rows.append(rows)
    
    return [file_artists,file_albums,file_tracks,file_customers,file_playlists, file_rows]
 
 
def get_all_rows():
    filenames = os.listdir(SOURCE_PATH)
    all_rows = []
    total = len(filenames)
    for i,name in enumerate(filenames):
        print('\n \nSTARTING OPERATIONS %d / %d' %(i,total))
        filepath = SOURCE_PATH + name
        reader = read_data_chunk(filepath,25000000)
        rows = []
        for i,chunk in enumerate(reader):
            try:
                rows.append(len(chunk))
            except:
                print('%s error' %name)  
        all_rows.append(sum(rows))
    print('\n total rows: ', sum(all_rows))
    
    
def main():
    filenames = os.listdir(SOURCE_PATH)
    skipped_files = []
    total = len(filenames)
    all_artists = set()
    all_albums = set()
    all_tracks = set()
    all_customers = set()
    all_playlists = set()
    all_rows = []
    for i,name in enumerate(filenames):
        try:
            print('\n \nSTARTING OPERATIONS %d / %d' %(i,total))
            filepath = SOURCE_PATH + name
            file_data = get_file_data(filepath)
    
            # updating sets
            print('updating sets')
            all_artists.update(file_data[0])
            all_albums.update(file_data[1])
            all_tracks.update(file_data[2])
            all_customers.update(file_data[3])
            all_playlists.update(file_data[4])
            all_rows.append(sum(file_data[5]))
        except:
            skipped_files.append(name)
            print('error, skipping file')
            print(str(skipped_files))
            with open("/project/skipped.txt", "w") as output:
                output.write(str(skipped_files))
        
    # iteration finished
    print(len(all_artists))
    print(len(all_albums))
    print(len(all_tracks))
    print(len(all_customers))
    print('playlist:',len(all_playlists))
    print('rows:',sum(all_rows))
    
if __name__ == "__main__":
   #main()
   get_all_rows()
