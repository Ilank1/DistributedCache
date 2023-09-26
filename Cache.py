from bf import BloomFilter
import time, threading


# Nodes count
n = 20

# false positive probability
p = 0.05


class Node():
    def __init__(self, centralized, network, id=-1, data=None):
        self.network = network
        self.centralized = centralized
        self.id = id

        # Indicates whether the current node holds the key
        if data is None:
            data = {}

        self.current_keys = BloomFilter(n, p)
        for key in data.keys():
            self.current_keys.add(key)

        # Indicates if any other node in the network has the key
        self.peers_keys = BloomFilter(n, p)

        self.data = data

    # self.nodes_bfs =  [{"bf":BloomFilter(n, p),"index":i} for i in range(n)]

    def get(self, key):
        # Initially check local node cache, TODO: do we really need BF for that? Why not to use the dict?
        if self.current_keys.check(key) and key in self.data:
            return self.data[key]

        # If current node doesn't has the key, check the peers in the network
        else:
            if self.peers_keys.check(key):
                return self.network.broadcast_get(key)
            else:
                return self.centralized.get(key)


    def check_current(self, key):
        return

    def add_key(self, key, value):
        self.data[key] = value
        self.current_keys.add(key)

    def remove_key(self, key):
        raise NotImplemented("Need to think how to remove key from BF")


class Network():
    def __init__(self):
        self.nodes = []
        merging_thread = threading.Thread(target=self.run_bf_merge)
        merging_thread.start()
    def merge_network_bf(self):
        # Calculate the merged BF
        bf = self.nodes[0].current_keys.bit_array
        for node in self.nodes:
            bf |= node.current_keys.bit_array

        # Set if for all the nodes
        for node in self.nodes:
            node.current_keys.bit_array = bf

    def run_bf_merge(self):
        while True:
            time.sleep(0.1)
            #print(f"Merging BFs")
            self.merge_network_bf()




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
        time.sleep(1)

        if key in self.data:
            return self.data[key]
        else:
            raise Exception("Value doesn't exist in centralized cache")

    def set(self, key, value):
        self.data[key] = value

