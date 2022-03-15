import time


def download():
    print("Downloading data...")
    time.sleep(10)
    return 1


def merge(arg, arg2):
    print("Merging data...")
    time.sleep(1)
    return 1


def transform(arg):
    print("Transforming data...")
    time.sleep(4)
    return 1


def train(arg):
    print("Training LightGBM model...")
    time.sleep(9)
    return 0.89
