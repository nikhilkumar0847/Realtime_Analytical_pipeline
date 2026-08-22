import sys
import pandas as pd
import great_expectations as gx

def run_validation():
    # Read all parquet files in the folder at once
    df = pd.read_parquet("./data/silver/category_window_agg")
    
    # Print columns so you can confirm names
    print("Columns found:", df.columns.tolist())
    print("Row count:", len(df))

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(name="silver_datasource")
    data_asset = data_source.add_dataframe_asset(name="category_window_agg_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")

    suite = gx.ExpectationSuite(name="category_window_agg_suite")
    suite = context.suites.add(suite)

    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="category"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="window"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="event_count", min_value=0)
    )

    batch_parameters = {"dataframe": df}

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="silver_validation",
    )
    validation_definition = context.validation_definitions.add(validation_definition)

    checkpoint = gx.Checkpoint(
        name="silver_checkpoint",
        validation_definitions=[validation_definition],
    )
    checkpoint = context.checkpoints.add(checkpoint)

    result = checkpoint.run(batch_parameters=batch_parameters)

    if not result.success:
        print("DATA QUALITY CHECK FAILED")
        sys.exit(1)
    else:
        print("Data quality check passed.")

if __name__ == "__main__":
    run_validation()