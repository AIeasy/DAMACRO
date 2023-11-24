import time
import pandas as pd
import gzip
import io
from scipy.stats import entropy
import math
import itertools
from collections import defaultdict
from itertools import combinations
import numpy as np
import lz4.frame as lz4
import zstandard as zstd
import networkx as nx
from math import sqrt
import json
from collections import Counter
from FD_Mutual_Info import *
import argparse
from datetime import datetime


def Get_data (data_path, sample_num, random_seed, delimiter):

    df_original = pd.read_csv(data_path, header = 0, delimiter = delimiter)
    df = df_original.sample(n = sample_num, random_state=random_seed)
    df_sample = df.reset_index(drop=True)
    df = df.reset_index(drop=True)
    
    return df_original, df_sample


def Get_config_json_FD(config_file, save_result_path):
    with open(config_file, 'r') as file:
        config = json.load(file)

    data_path = config['data_path']
    sample_num = config['sample_num']
    sample_seed = config['sample_seed']
    eps_3 = config['epsilon_3']
    eps_2 = config['epsilon_2']
    prune_threshold = config['prune_threshold']
    data_name = config["data_name"]
    delimiter = config["delimiter"]

    return data_path, delimiter, sample_num, sample_seed, eps_3, eps_2, prune_threshold, data_name


def Find_FD(config_file_path, save_result_path):

    config_file_path

    data_path, delimiter, sample_num, sample_seed, eps_3, eps_2, prune_threshold, data_name = Get_config_json_FD(config_file_path)

    df_original, df_sample = Get_data(data_path, sample_num, sample_seed, delimiter)

    start_time = time.time()
    prune_df = prune_data(df_sample, eps_3, prune_threshold)
    FD_list = Entropy_Cal(prune_df, eps_2)
    end_time = time.time()

    time_taken = end_time - start_time

    G = nx.DiGraph()
    G.add_edges_from(FD_list)

    plot_graph(G, data_name)


    # Flatten the list
    flattened_data = [item for sublist in FD_list for item in sublist]

    # Count occurrences
    counter = Counter(flattened_data)

    # Sort items based on count
    sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)

    #算下这个list里面每个column name的数量，排序一下，然后按这个顺序排列
    print(sorted_items)
    sorted_list_column = [item[0] for item in sorted_items]
    print(sorted_list_column)

    selected = select_nodes(FD_list)

    lists_dict = {
        "time_taken": time_taken,
        'sorted_items': sorted_items,
        'sorted_list_column': sorted_list_column,
        'FD_list': FD_list,
        "Selected" : selected
    }
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M")
    file_path = "FD_result/" + data_name + str(current_time) + "_FD.json"
    with open(file_path, 'w') as file:
        json.dump(lists_dict, file)


if __name__ == '__main__':
    
    default_file_path = "config_folder/config_adult.json"
    
    parser = argparse.ArgumentParser(description='the config file path of k prototype clustering')
    parser.add_argument('--config_path', type=str, default=default_file_path, help='Input string')
    args = parser.parse_args()

    config_path = args.config_path
    
    Find_FD(config_path)





