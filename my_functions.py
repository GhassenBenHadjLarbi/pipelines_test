def download():
    import time
    print("Downloading data...")
    time.sleep(10)
    return 1


def merge(arg, arg2):
    import time
    print("Merging data...")
    time.sleep(1)
    return 1


def transform(arg):
    import time
    print("Transforming data...")
    time.sleep(4)
    return 1


def train(arg):
    import time
    print("Training LightGBM model...")
    time.sleep(9)
    return 0.89
