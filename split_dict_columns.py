import os
import glob
import json
from tqdm import tqdm
from natsort import natsorted
import pandas as pd
import multiprocessing as mp
# import psutil


def get_all_files(root_dir, contains=[''], extions=['']):
    found_files = []
    for rt_dir, dirs, files in os.walk(root_dir):
        for ext in extions:
            ext = ext.lower()
            ext_len = len(ext)
            for file in files:
                file_ext = file[-(ext_len):]
                # print(file)
                file_ext = file_ext.lower()
                if file_ext == ext:
                    file_name = os.path.join(rt_dir, file)
                    found_files.append(file_name)
                    # continue                    
                
        for con in contains:
            con = con.lower()
            con_len = len(con)
            for file in files:
                if con in os.path.basename(file):
                    file_name = os.path.join(rt_dir, file)
                    found_files.append(file_name)
    return found_files

def unfold_row_dict(row, result_list, column_name='visitor_home_cbgs'):

    if 'safegraph_place_id' in row.index:
        placekey = row["safegraph_place_id"]
    if 'placekey' in row.index:
        placekey = row["placekey"]
        

    a_dict = json.loads(row[column_name])
    result_list += list(zip([placekey] * len(a_dict.keys()), a_dict.keys(), a_dict.values()))
    
def split_to_county0(df, saved_path, column_name='visitor_home_cbgs', file_suffix=''):
    if len(file_suffix) > 0:
            file_suffix = "_" +  file_suffix 
            
    df['county_code'] = df[column_name].str.zfill(12).str[:5]
    county_list = df['county_code'].unique()
    
    county_list = [c for c in county_list if c.isnumeric()]
    county_list = sorted(county_list) 
    print("   len of county_list:", len(county_list))

    df_row_cnt = len(df)
    removed_cnt = 0
    for idx, county in enumerate(county_list):  # cannot use tqdm in multiprocessing!  
        
        # print(f"    PID {os.getpid()} is processing county: {county}, {idx + 1} / {len(county_list)} \n")
        idxs = df['county_code'] == county
        county_df = df[idxs]
        
        basename = f'{county}_{column_name}{file_suffix}.csv'
        state_code = basename[:2]
        
        new_name = os.path.join(saved_path, state_code, county, basename)

        dirname = os.path.dirname(new_name)  
        os.makedirs(dirname, exist_ok=True)        
        
        county_df = county_df[[column_name, "placekey", "visits"]].sort_values([column_name, 'visits'], ascending=[True, False])
        county_df.to_csv(new_name, index=False)
        removed_cnt += len(county_df)
        
        df = df[~idxs]
        
        # print(f"len of df after removing count_df: {len(df)}; removed rows: {df_row_cnt - len(df)}")
        
        
        if idx % 100 == 0:
            print(f"   PID {os.getpid()} finished {idx + 1} / {len(county_list)} counties for in period: {file_suffix}\n")

def unfold_df_columns(df, saved_path, file_suffix, columns=['visitor_home_cbgs', 'visitor_daytime_cbgs']):    
    for column in columns:
        pair_list = []
        print(f"   Creating {column} edges for {len(df)} POIs...")
        df.apply(unfold_row_dict, args=(pair_list, column), axis=1)
        pair_list_df = pd.DataFrame(pair_list)
        pair_list_df.columns = ["placekey", column, "visits"]

        print(f"   Created {len(pair_list_df)} {column} edges.")

        print(f"   Splitting {column} edges into county level...")
        os.makedirs(saved_path, exist_ok=True)
        split_to_county(df=pair_list_df, column_name=column, saved_path = saved_path, file_suffix=file_suffix)
        print(f"   Finish splitting {column} edges.")
        
def split_to_county(df, saved_path, column_name='visitor_home_cbgs', file_suffix=''):
    if len(file_suffix) > 0:
            file_suffix = "_" +  file_suffix 
            
    df['county_code'] = df[column_name].str.zfill(12).str[:5]
    county_list = df['county_code'].unique()
    
    county_list = [c for c in county_list if c.isnumeric()]
    county_list = sorted(county_list) 
    

    df_row_cnt = len(df)
    removed_cnt = 0
    
    groups = df.groupby('county_code', as_index=False)
    # print("   len of county_list:", len(county_list))
    processed_county_cnt = 0
    for county, county_df in groups:
        
        if not county.isnumeric():
            continue
        
        basename = f'{county}_{column_name}{file_suffix}.csv'
        state_code = basename[:2]
        
        new_name = os.path.join(saved_path, state_code, county, basename)

        dirname = os.path.dirname(new_name)  
        os.makedirs(dirname, exist_ok=True)        
        
        county_df = county_df[[column_name, "placekey", "visits"]].sort_values([column_name, 'visits'], ascending=[True, False])
        county_df.to_csv(new_name, index=False)
        removed_cnt += len(county_df)

        processed_county_cnt += 1
        if processed_county_cnt % 100 == 0:
            print(f"   PID {os.getpid()} finished {processed_county_cnt} / {len(groups)} counties for in period: {file_suffix}: {column_name}\n")
            
      

def _process_weekly_patterns(week_strings, weekly_csv_files, saved_path):  # single_process
    week_count_cnt = len(week_strings)
    processed_cnt = 0
    while len(week_strings) > 0:
        week_str = week_strings.pop(0)
        processed_cnt += 1
        print(f"Processing: {processed_cnt} / {week_count_cnt}: {week_str}")        
        print(f'Processing week_str: {week_str}, {processed_cnt} / {len(week_strings)}')
        df_list = []

        print(f"   Reading CSV files...")
        for f in weekly_csv_files:        
            if os.path.basename(f).startswith(week_str):
                # print(f)
                df = pd.read_csv(f)
                df_list.append(df)
                 

        weekly_df = pd.concat(df_list).iloc[:]
        start_date = weekly_df['date_range_start'].min()[:10] # E.g.: 2018-01-15T00:00:00-09:00
        end_date = weekly_df['date_range_end'].max()[:10]
        print(f"   Read {len(df_list)} files. Date range: {start_date} - {end_date}")
        file_suffix = f"{start_date}_To_{end_date}"

        # Unfold_columns    
        unfold_columns=['visitor_home_cbgs', 'visitor_daytime_cbgs']
        unfold_df_columns(df=weekly_df, saved_path=saved_path, file_suffix=file_suffix, columns=unfold_columns)   
        
        # Save POI CSV without the split columns.
        POI_new_name = os.path.join(saved_path, "POI", f"POI_{file_suffix}.csv")
        os.makedirs(os.path.dirname(POI_new_name), exist_ok=True)
        print(f"   Saving POI files to: {POI_new_name}")
        # POI_drop_columns = ['visitor_home_cbgs', 'visitor_daytime_cbgs']
        POI_drop_columns = unfold_columns
        weekly_df.drop(columns=POI_drop_columns).to_csv(POI_new_name, index=False)
        del weekly_df
        print()
    
    
def process_weekly_patterns_mp(week_strings, weekly_csv_files, save_dir, process_cnt = 3): 
    
    c
        
if __name__ == '__main__': 
    #process_raw_patterns()
    weekly_csv_dir = r'J:\weekly_patterns_20211211\to_cluster2'
    weekly_csv_files = get_all_files(root_dir=weekly_csv_dir, contains=['row'], extions=[''])
    weekly_csv_files = natsorted(weekly_csv_files, reverse=True)
    print(f"Found {len(weekly_csv_files)} files. ", weekly_csv_files[0])        
    # get week strings
    week_strings = [os.path.basename(f)[:10] for f in weekly_csv_files]
    week_strings = list(set(week_strings))
    week_strings = natsorted(week_strings, reverse=True)
    print(f"Found {len(week_strings)} weeks.", week_strings[0])
    
    # Start to process each week
    print("Start to process each week:\n")
    save_dir = r'J:\Safegraph\weekly_county_files\weekly_patterns_2018_2021'
    
    # _process_weekly_patterns(week_strings, weekly_csv_files, save_dir)
    process_weekly_patterns_mp(week_strings=week_strings[209:], weekly_csv_files=weekly_csv_files,
                               save_dir = save_dir,
                               process_cnt = 1)
    
    