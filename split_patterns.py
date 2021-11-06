# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import os
import glob
import json
import csv
import pandas as pd
import dask
import dask.dataframe as dd
import multiprocessing as mp
import psutil
from natsort import natsorted
import subprocess


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
    for idx, county_fips in enumerate(unique_county_fips):
        if county_fips[:2] != "45":  # SC: 45
            continue
        county_df = df[df['county_fips'] == county_fips]
        state = county_df.iloc[0][region_column]

        start_date = get_min_date(df, date_column='date_range_start')
        to_dir = os.path.join(saved_path, state, county_fips)
        os.makedirs(to_dir, exist_ok=True)
        file_name = os.path.join(to_dir, f"{county_fips}_{start_date }.csv")
        county_df.to_csv(file_name, index=False)

        df = df.drop(county_df.index)

        if idx % 500 == 0:
            print(f"    Processed {idx} counties for week: {start_date}.")



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
            print("    Reading CSV files...")
            for idx, csv in enumerate(csv_files[2:]):
                print("    Reading:", os.path.basename(csv))
                df = pd.read_csv(csv, engine='python', quoting=3, nrows=None, error_bad_lines=False)
                df = df.dropna(subset=['poi_cbg'])
                df = df[df['poi_cbg'].astype(str).str.startswith('45')] # for SC only
                # df = df[~df['poi_cbg'].astype(str).str.startswith('CA')] # drop: CA:59152171

                df['poi_cbg'].fillna(0)
                df['poi_cbg'] = df['poi_cbg'].astype(int).astype(str).str.zfill(12)
                dfs.append(df)

            df_all = pd.concat(dfs)
            print( "    Concatenating...")
            print(f"    Finished reading CSV files, obtained {len(df_all)} rows.")

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
    root_dir = r'H:\Safegraph\Weekly Places Patterns Backfill for Dec 2020 and Onward Release\patterns_backfill\2020\12\14\21'  # finished

    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Jan 2018 - Apr 2020'  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns'  # processed
    root_dir = r"H:\Safegraph\Monthly Places Patterns (aka Patterns) May 2020 - Nov 2020\patterns"  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns_backfill\2021\04\13'
    root_dir = r'H:\Safegraph\Weekly Places Patterns (for data from 2020-11-30 to Present)\patterns_backfill'
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns\2021\06\05\00'

    root_dir = r'/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns'

    found_files = get_all_files(root_dir, extions=[".gz"])
    dirs = get_dir_from_files(found_files)
    dirs = natsorted(dirs)

    # Error in /media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/04/07/06
    # Error info: field larger than field limit (131072)
    # field larger than field limit (131072) /media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/23/17
    dirs = [ '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/14/17',
            '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/04/07/06',
            '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/23/17',
            '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/30/18',
            '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/07/07/20',
            '/media/gpu/easystore/Safegraph/Weekly Places Patterns (for data from 2020-11-30 to Present)/patterns/2021/06/16/18',
            ]

    dirs_mp = mp.Manager().list()
    for d in dirs[:]:
        dirs_mp.append(d)

    print("os.cpu_count():", os.cpu_count())
    print("mp.cpu_count():", mp.cpu_count())
    print("psutil.cpu_count(logical = False):", psutil.cpu_count(logical=False))
    print("psutil.cpu_count(logical = True):", psutil.cpu_count(logical=True))

    process_cnt = 1

    pool = mp.Pool(processes=process_cnt)

    saved_path = r'/media/gpu/easystore/Safegraph_reorganized/county_weekly_patterns_split'

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


if __name__ == '__main__':
    process_raw_patterns()

    # patterns_CBG_all_csv(file_dir=r'H:\Safegraph', process_cnt=3)

    # rename_edge_csv()


