# Copyright 2024
# Author: Usamah Zaheer

import pytest
from pyspark.sql import SparkSession
from src.data_processing.transformer import PetitionTransformer
import os

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("Test") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_process_petitions(spark):
    """
    Test the process_petitions method of the PetitionTransformer class.

    This test verifies that the process_petitions method correctly processes
    the input data and generates the expected output CSV file.
    """
    transformer = PetitionTransformer(spark)
    
    input_path = "tests/test_data/test_input_data.json"
    output_dir_path = "tests/test_data/output_data"
    
    transformer.process_petitions(input_path, output_dir_path)
    
    # Check if the output CSV file is created
    assert os.path.exists(output_dir_path)
    
    # Load the output CSV and check its contents
    output_df = spark.read.csv(output_dir_path, header=True)

    assert output_df.count() > 0 # Check if there are any rows in the output
    assert "petition_id" in output_df.columns
    assert len(output_df.columns) == 21 # 20 words + 1 petition_id