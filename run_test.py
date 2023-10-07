from Cache import Node, Network, Centralized
from DataGenerator import SamplesGenerator, pick_random_keys
from timeit import default_timer as timer
import numpy as np
import os
from random import shuffle

num_of_keys = 10000
num_of_nodes = 100
num_of_queries = 2000

MyNetwork = Network()

# Should centralized know all the data?
samples = SamplesGenerator(num_of_keys)

data = samples.generate_data()
centralized = Centralized(data)

MyNetwork.nodes.extend(
    [Node(centralized, MyNetwork, 4, 1, pick_random_keys(data, 20)),
     Node(centralized, MyNetwork, 4, 2, pick_random_keys(data, 20))])

MyNetwork.nodes.extend([Node(centralized, MyNetwork, 4, i + 2) for i in range(num_of_nodes)])

# For 2 manually added nodes

num_of_nodes += 2


def only_local_cache():
    # every node has only local each vs network cache

    start = timer()

    for query in samples.generate_queries(num_of_queries):
        try:
            # Use other dist for nodes selection?
            MyNetwork.nodes[np.random.randint(0, num_of_nodes)].get(query, check_network=False)

            #print(f"{query} : {MyNetwork.nodes[np.random.randint(0, num_of_nodes)].get(query, check_network=False)}")

        except Exception as e:
            print(f"Exception: {e}")

    end = timer()
    print(f"only_local_cache, time: {end - start}")


def always_broadcast():
    # ignore the merged BF and broadcast anyway if the key doesn’t exist locally
    # every node has only local each vs network cache

    start = timer()

    for query in samples.generate_queries(num_of_queries):
        try:
            # Use other dist for nodes selection?
            MyNetwork.nodes[np.random.randint(0, num_of_nodes)].get(query, force_broadcast=True)

            # print(f"{query} : {MyNetwork.nodes[np.random.randint(0, num_of_nodes)].get(query, force_broadcast=True)}")
        except Exception as e:
            print(f"Exception: {e}")

    end = timer()
    print(f"always_broadcast, time: {end - start}")


def use_network_cache_and_merged_bf():
    # every node has only local each vs network cache

    start = timer()

    for query in samples.generate_queries(num_of_queries):
        try:
            # Use other dist for nodes selection?
            MyNetwork.nodes[np.random.randint(0, num_of_nodes)].get(query)

            # print(f"{query} : {MyNetwork.nodes[np.random.randint(0, num_of_nodes)].get(query)}")
        except Exception as e:
            print(f"Exception: {e}")

    end = timer()
    print(f"use_network_cache_and_merged_bf, time: {end - start}")


only_local_cache()
always_broadcast()
use_network_cache_and_merged_bf()
