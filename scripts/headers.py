import pandas as pd 
import os
import numpy as np

SOURCE_PATH = '/project/raw_data/'
DESTINATION_PATH = '/project/pickles/'

def put_headers(df):
    col_names = ['date','logtime','track_id','artist_name','track_name',
    'album_name','customer_id','access', 'gender', 'birth_year', 
    'region_code', 'stream_length',
    'stream_source', 'stream_source_uri', 'stream_device', 
    'stream_os', 'track_uri','track_artists']
    df.columns = col_names
    return df
    
def set_col_types(df):
    #df['date'] = pd.to_datetime(df.date, errors='coerce').dt.date
    df = df[['birth_year','stream_length']].apply(pd.to_numeric, errors='ignore',downcast='float')
    return df

def col_converter(x):
    try: 
        return float(x)
    except ValueError: 
        return np.nan
        
def read_data_chunk(filepath,col_converter,chunksize):
    # for testing only
    col_types = {14:col_converter,22:col_converter}
    chunks = pd.read_table(filepath,header=None, compression='gzip',
                  usecols=[0,1,3,6,7,8,9,11,13,14,16,22,24,25,26,27,28,29],
                  converters=col_types,parse_dates=[0,1],sep='\x01',chunksize=chunksize,encoding='latin-1',error_bad_lines=False)
    return chunks
    
def read_data(filepath):
    col_types = {14:np.float16,22:np.float16}
    df = pd.read_table(filepath,header=None,compression='gzip',
                  usecols=[0,1,3,6,7,8,9,11,13,14,16,22,24,25,26,27,28,29],
                  dtype=col_types,parse_dates=[0,1],sep='\x01',encoding='latin-1',error_bad_lines=False)
    return df
    
def run_operations_chunk(filepath,col_converter,chunksize=25000000):
    print('1/3 reading data')
    DESTINATION_PATH = '/project/pickles/'
    reader = read_data_chunk(filepath,col_converter,chunksize)
    for i,chunk in enumerate(reader):
        print('2/4 naming headers')
        df = put_headers(chunk)
        #set column types to save memory
        #print('3/4 changing column types')
        #df = set_col_types(df)
        # save output
        output_name = DESTINATION_PATH + name[:-3] +'_' + str(i) + '.pickle'  
        print('4/4 saving files')
        df.to_pickle(output_name)
    print(output_name + ' SAVED')

def run_operations(filepath, isChunk=False):
    print('1/4 reading data')
    if isChunk==True:
        # change output dir to test folder
        DESTINATION_PATH = '/project/test_chunk/'
        df = read_data_chunk(filepath)
    elif isChunk==False:
        df = read_data(filepath)
    # put headers
    print('2/4 naming headers')
    df = put_headers(df)
    # set column types to save memory
    print('3/4 changing column types')
    df = set_col_types(df)
    # save output
    output_name = DESTINATION_PATH + name
    print('4/4 saving files')
    df.to_csv(output_name, sep=',', index=False)
    print(output_name + ' SAVED')

if __name__ == "__main__":
    filenames = os.listdir(SOURCE_PATH)
    skipped_files=[]
    #done_files = os.listdir(DESTINATION_PATH)
    #done_files = [f[:-9] for f in done_files]
    #filenames = [f for f in filenames if f[:-3] not in done_files]
    print('first 5 files:',filenames[:5])
    # open data, select columns, set headers, and save into csv in /data
    for name in filenames:
        print('STARTING OPERATIONS FOR', name)
        filepath = SOURCE_PATH + name
        try:
            run_operations_chunk(filepath,col_converter)
        except:
            skipped_files.append(name)
            print('error, skipping file')
            print(str(skipped_files))
            with open("/project/skipped.txt", "w") as output:
                output.write(str(skipped_files))
        
    
    