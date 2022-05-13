# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import os
import glob
import json
import csv
import pandas as pd
# import dask
# import dask.dataframe as dd
import multiprocessing as mp
import psutil
from natsort import natsorted
import subprocess
import sys
import  numpy as np

# print("sys.maxsize:", sys.maxsize)

def get_all_files(root_dir, extions=[".gz"]):
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

    return found_files


def get_dir_from_files(files):
    dirs = [os.path.dirname(f) for f in files]
    dirs = list(set(dirs))
    return dirs

def get_unique_fips(df, column, left_len):
    unique_fips = df[column].astype(str).str[:left_len].unique()
    unique_fips = natsorted(unique_fips)
    return unique_fips

def get_min_date(df, date_column='date_range_start'):
    # date = df.sort_values(date_column).iloc[0][date_column]
    date = df[date_column].min()
    date = date[:10]
    return date


def split_pois_to_county(df, saved_path):

    unique_county_fips = get_unique_fips(df, column='poi_cbg', left_len=5)
    df['county_fips'] = df['poi_cbg'].astype(str).str[:5]
    region_column = 'region'
    skipped_county_cnt = 0
    for idx, county_fips in enumerate(unique_county_fips):
        if county_fips[:2] != "45":  # SC: 45
            continue
        try:
            county_df = df[df['county_fips'] == county_fips]
            if len(county_df) == 0:
                skipped_county_cnt += 1
                print(f"    {os.getpid()} find no records for county FIPS: ", county_fips, '. Skipped.')
                print(f"    {os.getpid()} has skipped {skipped_county_cnt} counties. ")
                continue
            state = county_df.iloc[0][region_column]

            start_date = get_min_date(df, date_column='date_range_start')
            to_dir = os.path.join(saved_path, state, county_fips)
            os.makedirs(to_dir, exist_ok=True)
            file_name = os.path.join(to_dir, f"{county_fips}_{start_date }.csv")
            county_df.to_csv(file_name, index=False)


            print(f"    Found {len(county_df): >5} rows for county {county_fips} in {start_date}.")

            df = df.drop(county_df.index)

            if idx % 500 == 0:
                print(f"    Processed {idx} counties for week: {start_date}.")

        except Exception as e :
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = exception_traceback.tb_frame.f_code.co_filename
            line_number = exception_traceback.tb_lineno
            print("Error in split_pois_to_county() for loop:", e, county_fips, county_df)
            print("Linenumber: ", line_number, filename)
            continue


def process_dir(dirs, saved_path):
    total = len(dirs)
    while len(dirs) > 0:

        try:
            d = dirs.pop(0)

            print("Processing directory: ", d)
            # print(idx, csv)
            csv_files = glob.glob(os.path.join(d, "*csv.gz"))
            csv_files = natsorted(csv_files)

            # print(csvs)
            dfs = []
            pid_str = str(os.getpid())
            print(f"    PID {pid_str: >6} reading CSV files...")
            for idx, csv in enumerate(csv_files[:]):

                print(f"    PID {pid_str: >6} reading:", os.path.basename(csv))
                df = pd.read_csv(csv)
                df = df.dropna(subset=['poi_cbg'])
                df = df[df['poi_cbg'].astype(str).str.startswith('45')] # for SC only
                # df = df[~df['poi_cbg'].astype(str).str.startswith('CA')] # drop: CA:59152171

                df['poi_cbg'].fillna(0)
                df['poi_cbg'] = df['poi_cbg'].astype(float).astype('int64').astype(str).str.zfill(12)
                # print("df['poi_cbg']: ", df.iloc[0]['poi_cbg'])
                dfs.append(df)

            df_all = pd.concat(dfs)
            print( f"    PID {os.getpid()} concatenating...")
            print(f"    PID {os.getpid()} finished reading CSV files, obtained {len(df_all)} rows.")

            if not 'safegraph_place_id' in list(df_all.columns):
                df_all['safegraph_place_id'] = ''
                df_all['parent_safegraph_place_id'] = ''

            if not 'placekey' in list(df_all.columns):
                df_all['placekey'] = ''
                df_all['parent_placekey'] = ''

            split_pois_to_county(df_all, saved_path)

            print(f"Processed {total - len(dirs)} / {total}.")
        except Exception as e:
            print("Error in process_dir() while loop:", e, d, csv)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = exception_traceback.tb_frame.f_code.co_filename
            line_number = exception_traceback.tb_lineno
            print("Error in split_pois_to_county() for loop:", e, county_fips, county_df)
            print("Linenumber: ", line_number, filename)
            continue


# H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/14/17/patterns-part3.csv.gz

def unfold_row_dict(row, result_list):
    # print(type(row))
    # print(row.index)

    if 'safegraph_place_id' in row.index:
        placekey = row["safegraph_place_id"]

    if 'placekey' in row.index:
        placekey = row["placekey"]

    a_dict = json.loads(row["visitor_home_cbgs"])
    result_list += list(zip([placekey] * len(a_dict.keys()), a_dict.keys(), a_dict.values()))


def generate_edge_file_name(root_dir, dataset="monthly_pattern_backfill"):
    # print(dataset)
    if dataset == "monthly_pattern_backfill":
        # print(root_dir)
        month = root_dir[-7:].replace("\\", "-")
        basename = f"{dataset}_edges_{month}.csv"
        file_name = os.path.join(root_dir, basename)
        return (file_name)

    if dataset == "monthly_pattern_to_present":
        # print(root_dir)
        month = root_dir[-13:-3].replace("\\", "-")
        basename = f"{dataset}_edges_{month}.csv"
        file_name = os.path.join(root_dir, basename)
        return (file_name)

    if dataset == "weekly_pattern_backfill":
        # print(root_dir)
        date = root_dir[-10:].replace("\\", "-")
        basename = f"{dataset}_edges_{date}.csv"
        file_name = os.path.join(root_dir, basename)
        return (file_name)

    if dataset == "weekly_pattern_to_present":
        # print(root_dir)
        date = root_dir[-13:-3].replace("\\", "-")
        basename = f"{dataset}_edges_{date}.csv"
        file_name = os.path.join(root_dir, basename)
        return (file_name)

    else:
        return ""


def process_raw_patterns():
    saved_path = r'H:\Safegraph_reorganized\county_weekly_patterns_2021_release'

    # root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Jan 2018 - Apr 2020'
    # root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns'
    # root_dir = r"H:\Safegraph\Monthly Places Patterns (aka Patterns) May 2020 - Nov 2020\patterns"
    # root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns_backfill\2021\04\13'
    # root_dir = r'H:\Safegraph\Weekly Places Patterns (for data from 2020-11-30 to Present)\patterns_backfill'
    # root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns\2021\06\05\00'

    root_dir = r'H:\Safegraph\Weekly Places Patterns (for data from 2020-11-30 to Present)\patterns'  # processed
    # root_dir = r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\patterns_backfill\2020\12\14\21'  # finished
    # root_dir = r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\release-2021-07\weekly\patterns_backfill\2021\07\15\15\2020'
    # root_dir = r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\release-2021-07\weekly\patterns_backfill\2021\07\15\15\2021'


    found_files = get_all_files(root_dir, extions=[".gz"])
    dirs = get_dir_from_files(found_files)
    dirs = natsorted(dirs, reverse=True)[:]

    # Error in /media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/04/07/06
    # Error info: field larger than field limit (131072)
    # field larger than field limit (131072) /media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/23/17
    # dirs = [ '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/14/17',
    #         '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/04/07/06',
    #         '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/23/17',
    #         '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/30/18',
    #         '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/07/20',
    #         '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/16/18',
    #         ]
    #
    # dirs = [r'H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/14/17',
    #         r'H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/04/07/06',
    #         r'H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/23/17',
    #         r'H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/30/18',
    #         r'H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/07/20',
    #         r'H:/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/16/18',
    #         ]

    dirs_mp = mp.Manager().list()
    for d in dirs[:]:
        dirs_mp.append(d)

    print("Directory count: ", len(dirs_mp))

    print("os.cpu_count():", os.cpu_count())
    print("mp.cpu_count():", mp.cpu_count())
    print("psutil.cpu_count(logical = False):", psutil.cpu_count(logical=False))
    print("psutil.cpu_count(logical = True):", psutil.cpu_count(logical=True))

    process_cnt = 3

    pool = mp.Pool(processes=process_cnt)



    for i in range(process_cnt):
        pool.apply_async(process_dir, args=(dirs_mp, saved_path))
    pool.close()
    pool.join()


def rename_edge_csv():
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Jan 2018 - Apr 2020'  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns'  # processed
    root_dir = r"H:\Safegraph\Monthly Places Patterns (aka Patterns) May 2020 - Nov 2020\patterns"  # processed
    root_dir = r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\patterns_backfill\2020\12\14\21'  # finished
    root_dir = r'H:\Safegraph\Weekly Places Patterns (for data from 2020-11-30 to Present)\patterns'  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns'  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns_backfill\2021\04\13'

    found_files = get_all_files(root_dir, extions=[".csv"])
    print(found_files)
    dirs = get_dir_from_files(found_files)
    dirs = natsorted(dirs)

    print("len(found_files):", len(found_files))

    print(dirs)
    for d in dirs:
        file_name = generate_edge_file_name(d, dataset="monthly_pattern_backfill")
        # old_name = os.path.join(d, "edges.csv")
        old_name = glob.glob(os.path.join(d, "*.csv"))[0]
        os.rename(old_name, file_name)
        print(file_name)


def groupy_counties(edge_csv_list):
    #    if not isinstance(edge_csv_list, list):  # not suitable for mp.list()
    #        edge_csv_list= [edge_csv_list]
    total_cnt = len(edge_csv_list)
    saved_path = r'H:\Safegraph_reorganized\county_monthly_patterns'

    while len(edge_csv_list) > 0:
        # print(len(edge_csv_list))
        edge_csv_file = edge_csv_list.pop(0)
        print(f"Processing {total_cnt - len(edge_csv_list)} / {total_cnt}:", edge_csv_file)
        df = pd.read_csv(edge_csv_file, dtype={'visitor_home_cbgs': str})

        df['county_code'] = df['visitor_home_cbgs'].str.zfill(12).str[:5]
        county_list = df['county_code'].unique()

        # print("len of county_list:", len(county_list))

        df_row_cnt = len(df)

        removed_cnt = 0

        for idx, county in enumerate(county_list):  # cannot use tqdm in multiprocessing!
            # print(idx, county)
            basename = os.path.basename(edge_csv_file)
            # print("basename:", basename)

            new_name = f'County_{county}_{basename}'
            new_name = os.path.join(saved_path, new_name)
            # print("new_name:", new_name)
            idxs = df['county_code'] == county
            county_df = df[idxs]
            county_df.to_csv(new_name, index=False)
            # print("len of county_df:", len(county_df))

            removed_cnt += len(county_df)

            df = df[~idxs]
            # print("len of df after removing count_df:", len(df), df_row_cnt - removed_cnt)


def patterns_CBG_all_csv(file_dir=r'H:\Safegraph', process_cnt=3):
    out = subprocess.getoutput(f"dir  /b/n/s {file_dir}\monthly*edges*.csv")

    all_files = out.split("\n")

    print("Found files:", len(all_files))

    if process_cnt == 1:
        groupy_counties(all_files)

    if process_cnt > 1:
        all_files_mp = mp.Manager().list()
        for f in all_files[0:]:
            all_files_mp.append(f)

    # print(len(all_files_mp))

    pool = mp.Pool(processes=process_cnt)
    for i in range(process_cnt):
        print('starting process:', i)
        pool.apply_async(groupy_counties, args=(all_files_mp,))
    pool.close()
    pool.join()

    # process_dir(dirs[:])


# Press the green button in the gutter to run the script.

def pattern_poi_split_to_county(file_dir=r'H:\Safegraph', process_cnt=3):
    out = subprocess.getoutput(f"dir  /b/n/s {file_dir}\monthly*edges*.csv")

    all_files = out.split("\n")

    print("Found files:", len(all_files))

    if process_cnt == 1:
        groupy_counties(all_files)

    if process_cnt > 1:
        all_files_mp = mp.Manager().list()
        for f in all_files[0:]:
            all_files_mp.append(f)

    # print(len(all_files_mp))

    pool = mp.Pool(processes=process_cnt)
    for i in range(process_cnt):
        print('starting process:', i)
        pool.apply_async(groupy_counties, args=(all_files_mp,))
    pool.close()
    pool.join()


def extract_placekeys_visits():
    root_dir =  r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\release-2021-07\weekly\patterns_backfill\2021\07\15\15\2021'
    root_dir2 = r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\release-2021-07\weekly\patterns_backfill\2021\07\15\15\2020'

    saved_path = r'H:\Safegraph_reorganized\Placekeys_all_weekly_patterns_2021_release'
    found_files = get_all_files(root_dir, extions=[".gz"])
    found_files2 = get_all_files(root_dir2, extions=[".gz"])
    dirs1 = get_dir_from_files(found_files)
    dirs2 = get_dir_from_files(found_files2)

    dirs1 = natsorted(dirs1, reverse=True)[:]
    dirs2 = natsorted(dirs2, reverse=True)[:]
    dirs = dirs1 + dirs2
    dirs = dirs[:]

    usecol_old = ['placekey', 'parent_placekey', 'safegraph_place_id', 'parent_safegraph_place_id', 'raw_visitor_counts']
    usecol_new = ['placekey', 'parent_placekey', 'raw_visitor_counts']

    drop_colums = ['street_address', 'city', 'postal_code', 'date_range_start',
                   'date_range_end', 'visits_by_day', 'visits_by_each_hour',
                   'visitor_home_cbgs', 'visitor_daytime_cbgs', 'visitor_country_of_origin', 'distance_from_home',
                   'median_dwell']

    concated_df = None
    is_first_df = True

    total = len(dirs)
    while len(dirs) > 0:
        try:
            d = dirs.pop(0)
            print(f"Processing directory {total - len(dirs)} / {total}: ", d)
            # print(idx, csv)
            csv_files = glob.glob(os.path.join(d, "*csv.gz"))
            csv_files = natsorted(csv_files)

            # print(csvs)

            pid_str = str(os.getpid())
            print(f"    PID {pid_str: >6} reading CSV files...")

            dfs = []
            for idx, csv in enumerate(csv_files[:]):

                print(f"    PID {pid_str: >6} reading:", os.path.basename(csv))

                csv_columns = pd.read_csv(csv, nrows=0).columns.to_list()
                if 'safegraph_place_id' in csv_columns:
                    usecols = usecol_old
                else:
                    usecols = usecol_new

                df = pd.read_csv(csv, usecols=usecols, nrows=None, dtype={'raw_visitor_counts': int})
                # df['raw_visitor_counts'] = np.array(df['raw_visitor_counts']).astype(int)
                dfs.append(df)

            df = pd.concat(dfs, axis=0)
            # df['raw_visitor_counts'] = np.array(df['raw_visitor_counts']).astype(int)
            df = df.set_index('placekey')

            # get the start date
            # dir_name = os.path.dirname(d)  # No need for 2021-07 backfill released
            dir_name = d
            start_date = dir_name[-10:].replace('\\', r'-').replace(r'/', r'-')
            new_column_name = f"visitor_{start_date}"
            df = df.rename(columns={'raw_visitor_counts': new_column_name})
            #                 print(f"    Start_date: {start_date}", df.columns)

            if is_first_df:
                concated_df = df
                is_first_df = False
                # print(df.head(1))
                print(
                    f'    PID {os.getpid()}: Current rows counts: {len(concated_df)}, previous rows: {0}. Added {len(concated_df)} rows.')

                continue
            # print(df.dtypes, df)
            old_row_cnt = len(concated_df)
            concated_df = concated_df.merge(df[[new_column_name]], how='outer', left_index=True, right_index=True)
            # concated_df[new_column_name] = concated_df[new_column_name].fillna(0).astype(int)
            # concated_df = pd.concat([concated_df, df]).drop_duplicates(subset='placekey').reset_index(drop=True)

            new_row_cnt = len(concated_df)
            print(
                f'    PID {os.getpid()}: Current rows counts: {new_row_cnt}, previous rows: {old_row_cnt}. Added {new_row_cnt - old_row_cnt} rows, removed {old_row_cnt + len(df) - new_row_cnt} duplated rows.')

            print(f"Processed {total - len(dirs)} / {total}.")

        except Exception as e:
            print("Error in process_dir() while loop:", e, d, csv)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = exception_traceback.tb_frame.f_code.co_filename
            line_number = exception_traceback.tb_lineno
            print("Linenumber: ", line_number, filename)

    # compute the all visitors
    vistors_np = concated_df.iloc[:, -total:]
    vistors_np = np.nan_to_num(vistors_np, nan=0)

    concated_df['total_visitors'] = vistors_np.sum(axis=1).astype(int)
    # print(concated_df.drop(columns=usecols[:4]))
    new_name = os.path.join(saved_path, "all_placekey.csv")
    if not os.path.exists(saved_path):
        os.makedirs(saved_path)
    vistors_df = concated_df.drop(columns=['parent_placekey'])
    try:
        vistors_df = vistors_df.drop(columns=['safegraph_place_id'])
        vistors_df = vistors_df.drop(columns=['parent_safegraph_place_id'])
        #     usecol_old = ['placekey', 'parent_placekey', 'safegraph_place_id', 'parent_safegraph_place_id', 'raw_visitor_counts']
    except:
        pass
    vistors_df = vistors_df.fillna(0).astype(int)
    placekey_id_df = concated_df.drop(columns=vistors_df.columns)
    concated_df = pd.concat((placekey_id_df, vistors_df), axis=1)
    concated_df = concated_df.sort_values('total_visitors', ascending=False)
    concated_df.to_csv(new_name, index=True)
    print("Done.")
    return concated_df




if __name__ == '__main__':
    # process_raw_patterns()
    extract_placekeys_visits()
    # patterns_CBG_all_csv(file_dir=r'H:\Safegraph', process_cnt=3)

    # rename_edge_csv()


