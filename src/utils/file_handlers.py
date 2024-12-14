# Copyright 2024
# Author: Usamah Zaheer

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, split, element_at, concat_ws

class FileHandler:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_json(self, input_path: str) -> DataFrame:
        """
        Read a JSON file and add unique petition_id with first words from abstract and label.

        Parameters:
        input_path (str): The path to the input JSON file.

        Returns:
        DataFrame: A DataFrame containing the processed data with unique petition IDs.
        """
        df = self.spark.read.json(input_path)
        df = df.withColumn('label', df.label._value) \
               .withColumn('abstract', df.abstract._value) \
               .withColumn('petition_id', monotonically_increasing_id())

        # Check for missing or null values in important columns
        missing_count = df.filter(df.label.isNull() | df.abstract.isNull()).count()
        if missing_count > 0:
            logger.warning(f"Found {missing_count} rows with missing values in 'label' or 'abstract' columns.")

        return df

    def write_csv(
        self, 
        df: DataFrame, 
        output_path: str, 
        mode: str = "overwrite"
    ) -> None:
        """
        Write a DataFrame to a CSV file.

        Parameters:
        df (DataFrame): The DataFrame to write to CSV.
        output_path (str): The path to the output CSV file.
        mode (str): The write mode (default is "overwrite").

        Returns:
        None
        """
        df.write.mode(mode).csv(output_path, header=True)

