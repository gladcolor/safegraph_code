import os
import glob
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm



def get_visits_csv_files(target_state_code_list, visits_dir, start_date):
    visitor_home_csv_list = []
    visitor_daytime_csv_list = []
    try:
        for state in target_state_code_list:
            state_dir = os.path.join(visits_dir, state)
            print(f"    PID: {os.getpid()} is reading state dir:", state_dir, start_date)

            daytime_files = glob.glob(os.path.join(state_dir, r'*', fr'*visitor_daytime_cbgs_{start_date}*.csv'))
            visitor_daytime_csv_list += daytime_files
            home_cbgs_files = glob.glob(os.path.join(state_dir, r'*', fr'*visitor_home_cbgs_{start_date}*.csv'))
            visitor_home_csv_list += home_cbgs_files
    except Exception as e:
        print("Error in get_visits_csv_files():", e)

    return visitor_home_csv_list, visitor_daytime_csv_list


def process_weekly_strings(weekly_strings, target_state_code_list, weekly_POI_dir, save_dir, core_POI_df, visits_dir):
    
    weekly_strings_cnt = len(weekly_strings)
    weekly_idx = 0
    while len(weekly_strings) > 0:
        weekly_str = weekly_strings.pop(0)
        # print(f"PID {os.getpid()} is week: {weekly_str}...\n ") 
#  
        # if weekly_str in skip_weeks:
            # print(f"Date {weekly_str} have skipped.")
        #     continue

        print(f"PID {os.getpid()} is processing: {weekly_str}, {weekly_idx + 1} / {len(weekly_strings)}" )

        weekly_csv = glob.glob(os.path.join(weekly_POI_dir, f'POI_{weekly_str}_To_*.csv'))[0]
        print(f"    PID {os.getpid()} is reading weekly POI CSV file: {weekly_csv}")    
        # weekly_POI_df = pd.read_csv(weekly_csv, nrows=100000)  # for test
        weekly_POI_df = pd.read_csv(weekly_csv)
        weekly_POI_df['mean_vistor_visits'] = weekly_POI_df['raw_visit_counts'] / weekly_POI_df['raw_visitor_counts']
        print(f"    PID {os.getpid()} read {len(weekly_POI_df)} rows in weekly POI CSV file: {os.path.basename(weekly_csv)}")    

        
        visitor_home_csv_list, visitor_daytime_csv_list = get_visits_csv_files(
                                                        target_state_code_list=target_state_code_list, 
                                                                              visits_dir=visits_dir, 
                                                                              start_date=weekly_str)
        print(visitor_home_csv_list)

        print(f"    PID {os.getpid()} is processing mobility matrix for the week, please wait several minutes: {weekly_str}... ") 
        
        for idx, home_csv in tqdm(enumerate(visitor_home_csv_list[:])):
            if idx % 10 == 0:
                print(f"    PID {os.getpid()} is processing: {idx}, {os.path.basename(home_csv)}")

            county_fips = os.path.basename(home_csv)[:5]

            visits_df = pd.read_csv(home_csv)
            visits_POI_df = visits_df.merge(weekly_POI_df, left_on='placekey', right_on='placekey')
            visits_core_POI_df = visits_POI_df.merge(core_POI_df, left_on='placekey', right_on='placekey')

            visits_core_POI_df['mean_vistor_visits'] = visits_core_POI_df['raw_visit_counts'] / visits_core_POI_df['raw_visitor_counts']
            visits_core_POI_df['home_cbg_visits'] = visits_core_POI_df['mean_vistor_visits'] * visits_core_POI_df['visits']
            visits_core_POI_df['home_cbg_visits_time'] = visits_core_POI_df['home_cbg_visits'] * visits_core_POI_df['median_dwell']

            time_period = home_csv[-28:-4]
            new_name = os.path.join(save_dir, county_fips[:2], county_fips, f"{county_fips}_top_category_dwell_time_{time_period}.csv")
            os.makedirs(os.path.dirname(new_name), exist_ok=True)

            top_category_demian_dwell_df = visits_core_POI_df.groupby(['visitor_home_cbgs', 'top_category'], as_index=False).agg(top_catetory_time=('home_cbg_visits_time', 'sum'))
            top_category_demian_dwell_df.to_csv(new_name, index=False)    

            new_name = os.path.join(save_dir, county_fips[:2], county_fips, f"{county_fips}_sub_category_dwell_time_{time_period}.csv")
            os.makedirs(os.path.dirname(new_name), exist_ok=True)
            sub_category_demian_dwell_df = visits_core_POI_df.groupby(['visitor_home_cbgs', 'sub_category'], as_index=False).agg(top_catetory_time=('median_dwell', 'sum'))
            sub_category_demian_dwell_df.to_csv(new_name, index=False)
            
            weekly_idx += 1

        
        
if __name__ == '__main__': 
    
    print(f"Loading CORE POIs...")
    core_POI_csvs = glob.glob(r'J:\Safegraph\US_POI_20220103\*.csv.gz')[:]

    core_POI_df = pd.concat([pd.read_csv(c) for c in core_POI_csvs])
    columns = [c.replace("sg_c__", "") for c in core_POI_df.columns]
    core_POI_df.columns = columns

    usecols = ['placekey',
     'top_category',
     'sub_category',
     'naics_code',
     'latitude',
     'longitude',
     'phone_number',
     'open_hours',
     'category_tags',
     'opened_on',
     'closed_on',
     'tracking_closed_since',
     'geometry_type']

    core_POI_df = core_POI_df[usecols]
    
    weekly_POI_dir = r'J:\Safegraph\weekly_county_files\weekly_patterns_2018_2021\POI'
    weekly_csvs = glob.glob(os.path.join(weekly_POI_dir, f'POI_*.csv')) 
    weekly_csvs = sorted(weekly_csvs, reverse=True)

    weekly_strings = [os.path.basename(c)[4:14] for c in weekly_csvs]

    print(f"Found {len(weekly_strings)} weekly POI file.")
    
    save_dir = r'J:\Safegraph\weekly_county_files\weekly_patterns_time_aggre_2018_2021'
    visits_dir = r'J:\Safegraph\weekly_county_files\weekly_patterns_2018_2021'
    # target_state_code_list = ['45']

    for root, dirs, files in os.walk(r'J:\Safegraph\weekly_county_files\weekly_patterns_2018_2021'):
        target_state_code_list = dirs
        break
    target_state_code_list.remove('45')
    target_state_code_list.remove('POI')
    
    print(f"Need to process {len(target_state_code_list)} states.")

    # Start to process each week
    print("Start to process each week...\n")
    
        
    process_cnt = 5
    
    if process_cnt == 1:
        process_weekly_strings(weekly_strings, target_state_code_list, weekly_POI_dir, save_dir, core_POI_df, visits_dir)
            
    else:
        weekly_strings_mp = mp.Manager().list()
        for d in weekly_strings[:]:
            weekly_strings_mp.append(d)

        pool = mp.Pool(processes=process_cnt)

        for i in range(process_cnt):
            pool.apply_async(process_weekly_strings, args=(weekly_strings_mp, target_state_code_list, weekly_POI_dir, save_dir, core_POI_df, visits_dir))
        pool.close()
        pool.join()

        print("Done.")
    