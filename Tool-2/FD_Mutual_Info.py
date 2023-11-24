from collections import defaultdict
import matplotlib.pyplot as plt
import networkx as nx
import math
import pandas as pd
import numpy as np
from collections import defaultdict
from itertools import combinations
from datetime import datetime

#这个是计算co entropy，但如果df太长的话，会特别慢，如果可以最好还是用 chao shen estimator，但并不紧急
def joint_entropy(column1, column2):
    joint_prob = pd.crosstab(column1, column2, normalize=True)
    prob_array = joint_prob.values
    joint_ent = -np.nansum(prob_array*np.log(prob_array))
    return joint_ent

def entropy_single(meta_freq):
  total_elements = sum(meta_freq.values())  # total number of distinct elements
  entropy = 0

  for freq, count in meta_freq.items():
    probability = count / total_elements
    entropy -= probability * math.log2(probability)

  return entropy


#这个就是提前剪裁一些column，如果一个column的distinct number太少或者太多，都不行
def prune(df, epsilon_3, table_size):
    cols_to_drop = []
    for col in df:
        distinct_val = df[col].nunique()
        if (distinct_val < 3) or (distinct_val > (1 - epsilon_3) * table_size):
            cols_to_drop.append(col)

    df.drop(columns=cols_to_drop, inplace=True)
    return df

#删掉一些大多都是nan的column
def prune_nan(df, threshold):
  df_clean = df.dropna(axis=1, thresh=threshold)
  return df_clean


def prune_data(df, eps_3, threshold):
    table_len = len(df)
    threshold_length = threshold * table_len
    df_prune_nan = prune_nan(df, threshold)

    df_prune_eps_3 = prune(df_prune_nan, eps_3, table_len)

    return df_prune_eps_3

#这就是那个paper里面的algorithm，只是我没办法用chao shen，所以coentropy是硬算的
def compute_meta_frequency(freq):
    #Compute meta-frequency from a frequency table
    meta_freq = defaultdict(int)
    for key, value in freq.items():
        meta_freq[value] += 1
    return meta_freq


def Entropy_Cal(df, epsilon_2):
  cols = df.columns
  n = len(df)
  pair_list = []
  for col1, col2 in combinations(cols, 2):
      # Extract the two columns
      column_1 = df[col1]
      column_2 = df[col2]
      freqCi = defaultdict(int)
      freqCj = defaultdict(int)
      freqCi_Cj = defaultdict(int)
      for i in range(n):
        val1 = column_1.iloc[i]
        val2 = column_2.iloc[i]

        # Update frequency tables
        freqCi[val1] += 1
        freqCj[val2] += 1
        freqCi_Cj[(val1, val2)] += 1

        # Compute meta-frequency
        meta_freqCi = compute_meta_frequency(freqCi)
        meta_freqCj = compute_meta_frequency(freqCj)
        meta_freqCi_Cj = compute_meta_frequency(freqCi_Cj)

        # Check condition
        if len(freqCi_Cj) >= (1 + epsilon_2) * max(len(freqCi), len(freqCj)):
            break

      hci = entropy_single(meta_freqCi)
      hcj = entropy_single(meta_freqCj)
      #hpc = coentropy(meta_freqCi_Cj)
      hpc = joint_entropy(column_1, column_2)

      ucij = (hci + hcj - hpc)/hcj
      ucji = (hci - hcj - hpc)/hci
      if (ucij > epsilon_2) or (ucji > epsilon_2):
        pair_list.append((col1, col2))

  return pair_list


def find_subgraphs(edges):
    graph = defaultdict(list)
    for u, v in edges:
        graph[u].append(v)
        graph[v].append(u)

    visited = set()
    subgraphs = []

    def dfs(node, current_subgraph):
        visited.add(node)
        current_subgraph.append(node)
        for neighbor in graph[node]:
            if neighbor not in visited:
                dfs(neighbor, current_subgraph)

    for node in graph.keys():
        if node not in visited:
            current_subgraph = []
            dfs(node, current_subgraph)
            subgraphs.append(current_subgraph)

    return subgraphs, graph  # Return both subgraphs and the graph

def select_nodes(edges):
    subgraphs, graph = find_subgraphs(edges)  # Unpack both values

    if len(subgraphs) == 1:  # Whole graph
        node_degrees = {node: len(neighbors) for node, neighbors in graph.items()}
        sorted_nodes = sorted(node_degrees.keys(), key=lambda x: node_degrees[x], reverse=True)

        selected = []
        for node in sorted_nodes:
            if len(selected) == 3:
                break
            if all(other not in graph[node] for other in selected):
                selected.append(node)

        return selected

    else:  # There are subgraphs
        selected_nodes = []
        for subgraph in subgraphs:
            max_degree_node = max(subgraph, key=lambda node: len(graph[node]))
            selected_nodes.append(max_degree_node)

        return selected_nodes


def plot_graph(G, data_name):
    plt.figure(figsize=(15, 15))  # Set the figure size (increase if necessary)

    # Compute the node positions using the spring layout algorithm with adjusted spacing
    pos = nx.spring_layout(G, k=1.5/math.sqrt(G.order()))

    # Draw the graph with adjusted node and font sizes
    nx.draw(G, pos, with_labels=True, node_size=2500, node_color="skyblue", font_size=20, font_weight='bold', width=2.0, edge_color="gray")

    plt.title("Dependency Graph " + data_name)
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M")
    # Save the figure
    folder_path = "FD_Graphs/"
    graph_path = folder_path + data_name + current_time + ".png"
    plt.savefig(graph_path, format='png', bbox_inches='tight')

    # Show the plot
    plt.show()