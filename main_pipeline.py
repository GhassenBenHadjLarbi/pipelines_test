from clearml import PipelineDecorator


@PipelineDecorator.component(cache=False, repo='git@github.com:thepycoder/pipelines_test.git', repo_branch='main')
def download_data():
    # We want to import our packages INSIDE the function, so the agent knows what libraries to use when this function
    # becomes an isolated pipeline step
    from utils import download
    raw_data = download()
    return raw_data


@PipelineDecorator.component(cache=False, repo='git@github.com:thepycoder/pipelines_test.git', repo_branch='main')
def merge_data(raw_data, second_data_source="s3://second_data_source"):
    from utils import merge
    merged_data = merge(raw_data, second_data_source)
    return merged_data


@PipelineDecorator.component(cache=False, repo='git@github.com:thepycoder/pipelines_test.git', repo_branch='main')
def transform_data(merged_data):
    from utils import transform
    transformed_data = transform(merged_data)
    return transformed_data


@PipelineDecorator.component(cache=False, repo='git@github.com:thepycoder/pipelines_test.git', repo_branch='main')
def train_model(transformed_data):
    from utils import train
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
    # PipelineDecorator.run_locally()
    main("SELECT * FROM customers", "s3://my_data_bucket")
