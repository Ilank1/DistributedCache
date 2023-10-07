import matplotlib.pyplot as plt
from scipy.special import zeta
import numpy as np
import random
def pick_random_keys( a_dictionary , a_number ):
    _ = {}
    for k1 in random.sample( list( a_dictionary.keys() ) , a_number ):
        _[ k1 ] = a_dictionary[ k1 ]
    return _

class SamplesGenerator:
    def __init__(self, sample_size):
        self.key_prefix = "key_"
        self.value_prefix = "value_"
        self.sample_size = sample_size

    def generate_data(self):
        data = {}
        for i in range(self.sample_size):
            data[f"{self.key_prefix}{i}"] = f"{self.value_prefix}{i}"
        return data

    def generate_queries(self, num_of_queries, skew=None, zipf_dist_alpha=4.0, plot=False, uniform=False):
        # example netcache dists: 0.9, 0.95, 0.99

        if skew is not None:
            skew = float(skew)
            if skew >= 1 or skew <= 0:
                raise Exception(f"Skew {skew} invalid, should be (0,1)")

            zipf_dist_alpha = 1.0 / (1.0 - skew)

        if uniform:
            samples = np.random.randint(0, num_of_queries, num_of_queries)
        else:
            samples = np.random.zipf(zipf_dist_alpha, num_of_queries)

        if plot:
            count = np.bincount(samples)
            k = np.arange(1, samples.max() + 1)

            plt.bar(k, count[1:], alpha=0.5, label='sample count')
            plt.plot(k, num_of_queries * (k ** -zipf_dist_alpha) / zeta(zipf_dist_alpha), 'k.-', alpha=0.5,
                     label='expected count')
            plt.semilogy()
            plt.grid(alpha=0.4)
            plt.legend()
            plt.title(f'Zipf sample, a={zipf_dist_alpha}, size={num_of_queries}')
            plt.show()

        """
            Replace with generator for larger sizes, currently good enough
            for i in samples:
                yield f"{self.key_prefix}{i}"
        """
        return [f"{self.key_prefix}{i}" for i in samples]


s = SamplesGenerator(2000).generate_queries(num_of_queries=200, zipf_dist_alpha=1.9, plot=True)
