from clearml import PipelineDecorator, Task


@PipelineDecorator.component(cache=False, return_values=['raw_data'],
                             repo='https://github.com/GhassenBenHadjLarbi/pipelines_test.git', repo_branch='main')
def download_data(data_query, data_location):
    # We want to import our packages INSIDE the function, so the agent knows what libraries to use when this function
    # becomes an isolated pipeline step
    from src.my_functions import download
    raw_data = download(data_query, data_location)
    return raw_data


@PipelineDecorator.component(cache=False, return_values=['merged_data'],
                             repo='https://github.com/GhassenBenHadjLarbi/pipelines_test.git', repo_branch='main')
def merge_data(raw_data, second_data_source="s3://second_data_source"):
    from src.my_functions import merge
    merged_data = merge(raw_data, second_data_source)
    return merged_data


@PipelineDecorator.component(cache=False, return_values=['transformed_data'],
                             repo='https://github.com/GhassenBenHadjLarbi/pipelines_test.git', repo_branch='main')
def transform_data(merged_data):
    from src.my_functions import transform
    transformed_data = transform(merged_data)
    return transformed_data


@PipelineDecorator.component(cache=False, return_values=['accuracy'],
                             repo='https://github.com/GhassenBenHadjLarbi/pipelines_test.git', repo_branch='main')
def train_model(transformed_data):
    from src.my_functions import train
    accuracy = train(transformed_data)
    Task.current_task().get_logger().report_scalar('Accuracy', 'Validation', accuracy, iteration=0)
    return accuracy


@PipelineDecorator.pipeline(name="ETL Pipeline", project="Pipeline Examples", version="0.1")
def main(data_query, data_location):
    raw_data = download_data(data_query, data_location)
    merged_data = merge_data(raw_data)
    transformed_data = transform_data(merged_data)
    accuracy = train_model(transformed_data)
    PipelineDecorator.get_logger().report_scalar('Accuracy', 'Validation', accuracy, iteration=0)

    return accuracy


if __name__ == "__main__":
    PipelineDecorator.set_default_execution_queue('default')
    PipelineDecorator.debug_pipeline()
    main("SELECT * FROM customers", "s3://my_data_bucket")
