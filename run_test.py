from Cache import Node, Network, Centralized

from timeit import default_timer as timer

from random import shuffle

MyNetwork = Network()
MyCentralized = Centralized({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

MyNetwork.nodes.extend(
    [Node(MyCentralized, MyNetwork, 1, {"a": 1, "b": 2}), Node(MyCentralized, MyNetwork, 2, {"c": 3, "d": 4, "e": 5})])

MyNetwork.nodes.extend([Node(MyCentralized, MyNetwork, i + 2) for i in range(8)])

start = timer()

for key in ["a", "b", "c", "d", "e", "f", "g"]:
    try:
        print(MyNetwork.nodes[8].get(key))
    except Exception as e:
        print(e)

end = timer()
print(end - start)

# TODO: Understand what to compare to - each node knows about all others and syncs each other? how to compare runtimes? Standard benchmark?