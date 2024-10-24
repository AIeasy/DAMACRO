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
import os
from Test_compress_linear import *
from Test_compress_column import *

def remove_mismatched_types(df, expected_types):
    # Iterate over each row
    for index, row in df.iterrows():
        # Check each cell in the row
        for col, expected_type in zip(row, expected_types):
            if expected_type == 'int' and not pd.api.types.is_integer(col):
                # Drop the row if the type is not integer
                df.drop(index, inplace=True)
                break
            elif expected_type == 'str' and not isinstance(col, str):
                # Drop the row if the type is not string
                df.drop(index, inplace=True)
                break
    return df

#这个就是读一下data，然后取差不多一万个sample来算FD
def Get_data (data_path, sample_num, random_seed, delimiter):

    df_original = pd.read_csv(data_path, header = 0, delimiter = delimiter)
    df = df_original.sample(n = sample_num, random_state=random_seed)
    df_sample = df.reset_index(drop=True)
    df = df.reset_index(drop=True)
    
    return df_original, df_sample

#这个是读取config文件
def Get_config_json_FD(config_file):
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


def Find_FD(config_file_path):

    data_path, delimiter, sample_num, sample_seed, eps_3, eps_2, prune_threshold, data_name = Get_config_json_FD(config_file_path)

  
    df_original, df_sample = Get_data(data_path, sample_num, sample_seed, delimiter)
    

    start_time = time.time()

    prune_df = prune_data(df_original, eps_3, prune_threshold)
    FD_list = Entropy_Cal(prune_df, eps_2)
    end_time = time.time()

    time_taken = end_time - start_time

    current_time = time.time()
    folder_path = 'FD_result/' + data_name + str(current_time)
    os.makedirs(folder_path, exist_ok=True)

    G = nx.DiGraph()
    G.add_edges_from(FD_list)

    plot_graph(G, data_name, folder_path)


    flattened_data = [item for sublist in FD_list for item in sublist]
    counter = Counter(flattened_data)

    sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)

   
    print(sorted_items)
    sorted_list_column = [item[0] for item in sorted_items]
    print(sorted_list_column)
    

    
    selected_ThreeNode_split = select_nodes_ThreeEachSubgraph(FD_list, sorted_list_column)
    selected_TwoNode_split = select_nodes_TwoEachSubgraph(FD_list, sorted_list_column)
    selected_OneNode = select_nodes_OneEachSubgraph(FD_list, sorted_list_column)
    selected_ThreeNode = find_top_k_nodes(FD_list, 3, sorted_list_column)
    selected_TwoNode = find_top_k_nodes(FD_list, 2, sorted_list_column)


    file_path_compress = folder_path + "/" + data_name + "_CompressResult.json"

    try:
        file_size = os.path.getsize(data_path)
        print(f"File Size in Bytes is {file_size}")
    except FileNotFoundError:
        print("File not found.")
    except OSError:
        print("OS error occurred.")

    
    lists_dict = {
        "file_size" : file_size,
        "time_taken": time_taken,
        'sorted_items': sorted_items,
        'sorted_list_column': sorted_list_column,
        'FD_list': FD_list,
        "Selected_Three_node_split" : selected_ThreeNode_split,
        "Selected_Two_node_split": selected_TwoNode_split,
        "Selected_One_node" : selected_OneNode,
        "Selected_Three_node": selected_ThreeNode,
        "Selected_Two_node": selected_TwoNode
    }


    file_path_FD = folder_path + "/" + data_name + "_FD.json"
    with open(file_path_FD, 'w') as file:
        json.dump(lists_dict, file)

    compress_original_Linear(df_original, file_path_compress)
    compress_sortall_Linear(df_original, file_path_compress)
    compress_with_FD_Linear(df_original, selected_ThreeNode_split, file_path_compress, "Three_node_split")
    compress_with_FD_Linear(df_original, selected_TwoNode_split, file_path_compress, "Two_node_split")
    compress_with_FD_Linear(df_original, selected_OneNode, file_path_compress, "One_node")
    compress_with_FD_Linear(df_original, selected_ThreeNode, file_path_compress, "Three_node")
    compress_with_FD_Linear(df_original, selected_TwoNode, file_path_compress, "Two_node")

    Calculate_original_column_compress(df_original, folder_path)
    Calculate_Sortall_column_compress(df_original, folder_path)
    Calculate_FD_column_compress(df_original, selected_ThreeNode_split, folder_path, "selected_ThreeNode_split")
    Calculate_FD_column_compress(df_original, selected_TwoNode_split, folder_path, "selected_TwoNode_split")
    Calculate_FD_column_compress(df_original, selected_OneNode, folder_path, "selected_OneNode")
    Calculate_FD_column_compress(df_original, selected_ThreeNode, folder_path, "selected_ThreeNode")
    Calculate_FD_column_compress(df_original, selected_TwoNode, folder_path, "selected_TwoNode")





    


if __name__ == '__main__':
    
    default_file_path = "config_folder/config_df_electronics.json"
    
    parser = argparse.ArgumentParser(description='the config file path of k prototype clustering')
    parser.add_argument('--config_path', type=str, default=default_file_path, help='Input string')
    args = parser.parse_args()

    config_path = args.config_path
    
    Find_FD(config_path)







