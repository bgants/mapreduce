import multiprocessing
from functools import partial

def mapper(data):
    # Your map function logic goes here
    # It takes a piece of data and produces a list of key-value pairs
    return [(item, 1) for item in data]

def reducer(mapped_data):
    # Your reduce function logic goes here
    # It takes a key and a list of values, and produces a result
    return (mapped_data[0], sum(mapped_data[1])) 

def map_reduce(data, num_workers=2):
    # Split the data into chunks for parallel processing
    chunks = [data[i::num_workers] for i in range(num_workers)]

    # Use multiprocessing to parallelize the map step
    with multiprocessing.Pool(processes=num_workers) as pool:
        mapped_results = pool.map(mapper, chunks)

    # Flatten the list of mapped results
    mapped_data = [item for sublist in mapped_results for item in sublist]

    # Group the mapped data by key for the reduce step
    grouped_data = {}
    for key, value in mapped_data:
        if key not in grouped_data:
            grouped_data[key] = []
        grouped_data[key].append(value)

    # Use multiprocessing to parallelize the reduce step
    with multiprocessing.Pool(processes=num_workers) as pool:
        reduced_results = pool.map(reducer, grouped_data.items())

    return reduced_results

if __name__ == "__main__":
    # Example usage
    text = """
        Beautiful is better than ugly.
        Explicit is better than implicit.
        Simple is better than complex.
        Complex is better than complicated.
        Flat is better than nested.
        Sparse is better than dense.
        Readability counts.
        Special cases aren't special enough to break the rules.
        Although practicality beats purity.
        Errors should never pass silently.
        Unless explicitly silenced.
        In the face of ambiguity, refuse the temptation to guess.
        There should be one-- and preferably only one --obvious way to do it.
        Although that way may not be obvious at first unless you're Dutch.
        Now is better than never.
        Although never is often better than *right* now.
        If the implementation is hard to explain, it's a bad idea.
        If the implementation is easy to explain, it may be a good idea.
        Namespaces are one honking great idea -- let's do more of those!
        """
    data = text.split()
    result = map_reduce(data, num_workers=2)
    print("Final result:", result)