# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import os
import glob
import json

import pandas as pd
import dask
import dask.dataframe as dd
import multiprocessing as mp
import psutil
from natsort import natsorted


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


def process_dir(dirs):
    total = len(dirs)
    while len(dirs) > 0:
        d = dirs.pop(0)

        print("Processing: ", d)

        csvs = glob.glob(os.path.join(d, "*csv.gz"))
        saved_name = generate_edge_file_name(d, dataset="monthly_pattern_to_present")
        
        
        # print(csvs)
        dfs = []
        for csv in csvs:
            # print(csv)
            df = pd.read_csv(csv)
            dfs.append(df)
            
          

        df_all = pd.concat(dfs)

        pair_list = []

        df_all.apply(unfold_row_dict, args=(pair_list,), axis=1)

        pair_list_df = pd.DataFrame(pair_list)
        

        if 'safegraph_place_id' in list(df_all.columns):
            pair_list_df.columns = ["safegraph_place_id", "visitor_home_cbgs", "visits"]

        if 'placekey' in list(df_all.columns):            
            pair_list_df.columns = ["placekey", "visitor_home_cbgs", "visits"]
            
            
				# print("saved_name:", saved_name)
        print(f"Saving {saved_name}, number of edges: {len(pair_list_df)}")
        pair_list_df.to_csv(saved_name, index=False)      

        print(f"Processed {total - len(dirs)} / {total}.")


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
    root_dir = r'H:\Safegraph\Weekly Places Patterns (for data from 2020-11-30 to Present)\patterns'  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Jan 2018 - Apr 2020'  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns'  # processed
    root_dir = r"H:\Safegraph\Monthly Places Patterns (aka Patterns) May 2020 - Nov 2020\patterns"  # processed
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns_backfill\2021\04\13'
    root_dir = r'H:\Safegraph\Weekly Places Patterns (for data from 2020-11-30 to Present)\patterns_backfill'
    root_dir = r'H:\Safegraph\Monthly Places Patterns (aka Patterns) Dec 2020 - Present\patterns\2021\06\05\00'



    found_files = get_all_files(root_dir, extions=[".gz"])
    dirs = get_dir_from_files(found_files)
    dirs = natsorted(dirs)

    dirs_mp = mp.Manager().list()
    for d in dirs[:]:
        dirs_mp.append(d)

    print("os.cpu_count():", os.cpu_count())
    print("mp.cpu_count():", mp.cpu_count())
    print("psutil.cpu_count(logical = False):", psutil.cpu_count(logical=False))
    print("psutil.cpu_count(logical = True):", psutil.cpu_count(logical=True))

    process_cnt = 1

    pool = mp.Pool(processes=process_cnt)

    for i in range(process_cnt):
        pool.apply_async(process_dir, args=(dirs_mp,))
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
        #old_name = os.path.join(d, "edges.csv")
        old_name = glob.glob(os.path.join(d, "*.csv"))[0]
        os.rename(old_name, file_name)
        print(file_name)



    # process_dir(dirs[:])
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    process_raw_patterns()
    # rename_edge_csv()

