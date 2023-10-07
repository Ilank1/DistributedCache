from bf import BloomFilter
import time, threading
from collections import OrderedDict


class LRUCache:
    # TODO: should hold also valid bit for each value? in case centralized is changed?
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: int) -> int:
        if key not in self.cache:
            return -1
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: int, value: int) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)


class Node():
    def __init__(self, centralized, network, capacity: int, node_id=-1, initial_data=None):
        if initial_data is None:
            initial_data = dict()
        self.network = network
        self.centralized = centralized
        self.id = node_id
        self.local_cache = LRUCache(capacity)

        # Nodes count
        n = 20

        # false positive probability
        p = 0.05

        self.current_keys = BloomFilter(n, p)
        for key in initial_data.keys():
            # Add to cache
            self.local_cache.put(key, initial_data[key])

            #  Hold the key
            self.current_keys.add(key)

        # Indicates if any other node in the network has the key
        self.peers_keys = BloomFilter(n, p)

    # self.nodes_bfs =  [{"bf":BloomFilter(n, p),"index":i} for i in range(n)]

    def get(self, key, check_network=True, force_broadcast=False):
        # Initially check local node cache
        if self.current_keys.check(key) and key in self.local_cache.cache:
            return self.local_cache.get(key)

        # If current node doesn't have the key, check the peers in the network
        else:
            # Get from centralized without checking first local nodes
            if not check_network:
                return self.centralized.get(key)
            # Do broadcast without checking peers keys first
            if force_broadcast:
                return self.network.broadcast_get(key)
            else:
                if self.peers_keys.check(key):
                    return self.network.broadcast_get(key)
                else:
                    return self.centralized.get(key)

    def set(self, key, value):
        raise NotImplemented("Currently local node cache are initialized statically")

    def check_current(self, key):
        return self.current_keys.check(key)

    def add_key(self, key, value):
        self.local_cache.put(key, value)
        self.current_keys.add(key)

    def remove_key(self, key):
        raise NotImplemented("Need to think how to remove key from BF")


class Network():
    def __init__(self):
        self.nodes = []
        merging_thread = threading.Thread(target=self.run_bf_merge)
        merging_thread.start()

    def do_sharp_merge_network_bf(self):
        # Calculate the merged BF
        if len(self.nodes) > 0:
            bf = self.nodes[0].current_keys.bit_array
            for node in self.nodes:
                bf |= node.current_keys.bit_array

            # Set if for all the nodes
            for node in self.nodes:
                node.current_keys.bit_array = bf

    def run_bf_merge(self):
        while True:
            time.sleep(0.1)
            self.do_sharp_merge_network_bf()

    def broadcast_get(self, key):
        for node in self.nodes:
            if node.check_current(key):
                return node.get(key)


class Centralized():
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.data = data

    def get(self, key):
        # Make the centralized get cost more
        time.sleep(0.01)

        if key in self.data:
            return self.data[key]
        else:
            raise Exception("Value doesn't exist in centralized cache")

    def set(self, key, value):
        self.data[key] = value
