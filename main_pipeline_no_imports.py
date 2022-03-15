from clearml import PipelineDecorator


@PipelineDecorator.component(cache=False)
def download_data():
    import time
    # We want to import our packages INSIDE the function, so the agent knows what libraries to use when this function
    # becomes an isolated pipeline step
    def download():
        print("Downloading data...")
        time.sleep(10)
        return 1
    raw_data = download()
    return raw_data


@PipelineDecorator.component(cache=False)
def merge_data(raw_data, second_data_source="s3://second_data_source"):
    import time
    def merge(arg, arg2):
        print("Merging data...")
        time.sleep(1)
        return 1
    merged_data = merge(raw_data, second_data_source)
    return merged_data


@PipelineDecorator.component(cache=False)
def transform_data(merged_data):
    import time
    def transform(arg):
        print("Transforming data...")
        time.sleep(4)
        return 1
    transformed_data = transform(merged_data)
    return transformed_data


@PipelineDecorator.component(cache=False)
def train_model(transformed_data):
    import time
    def train(arg):
        print("Training LightGBM model...")
        time.sleep(9)
        return 0.89
    accuracy = train(transformed_data)
    return accuracy


@PipelineDecorator.pipeline(name="ETL Pipeline", project="Pipeline Examples", version="0.1")
def main(data_query, data_location):
    raw_data = download_data()
    merged_data = merge_data(raw_data)
    transformed_data = transform_data(merged_data)
    accuracy = train_model(transformed_data)

    return accuracy


if __name__ == "__main__":
    PipelineDecorator.set_default_execution_queue('default')
    PipelineDecorator.run_locally()
    main("SELECT * FROM customers", "s3://my_data_bucket")
