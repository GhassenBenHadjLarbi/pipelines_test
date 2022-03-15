from clearml import PipelineDecorator
from utils import download, merge, transform, train


@PipelineDecorator.component(cache=False, helper_functions=[download])
def download_data():
    # We want to import our packages INSIDE the function, so the agent knows what libraries to use when this function
    # becomes an isolated pipeline step
    raw_data = download()
    return raw_data


@PipelineDecorator.component(cache=False, helper_functions=[merge])
def merge_data(raw_data, second_data_source="s3://second_data_source"):
    merged_data = merge(raw_data, second_data_source)
    return merged_data


@PipelineDecorator.component(cache=False, helper_functions=[transform])
def transform_data(merged_data):
    transformed_data = transform(merged_data)
    return transformed_data


@PipelineDecorator.component(cache=False, helper_functions=[train])
def train_model(transformed_data):
    accuracy = train(transformed_data)
    return accuracy


@PipelineDecorator.pipeline(name="ETL Pipeline", project="Pipeline Getting Started", version="0.1")
def main(data_query, data_location):
    raw_data = download_data()
    merged_data = merge_data(raw_data)
    transformed_data = transform_data(merged_data)
    accuracy = train_model(transformed_data)

    return accuracy


if __name__ == "__main__":
    PipelineDecorator.set_default_execution_queue('default')
    # PipelineDecorator.run_locally()
    # print(ScriptInfo.get()[0].script)
    main("SELECT * FROM customers", "s3://my_data_bucket")
