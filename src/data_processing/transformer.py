# Copyright 2024
# Author: Usamah Zaheer

from pyspark.sql import SparkSession
from typing import List
import logging

from src.utils.file_handlers import FileHandler
from src.data_processing.text_processor import TextProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PetitionTransformer:
    """
    Class to process petitions data
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize the PetitionTransformer class
        """
        self.spark = spark
        self.file_handler = FileHandler(spark)
        self.text_processor = TextProcessor()

    def process_petitions(
        self, 
        input_path: str, 
        output_path: str
    ):
        """
        Process petitions data from the input path and write the results to the output path.

        Parameters:
        input_path (str): The path to the input JSON file containing petitions data.
        output_path (str): The path to the output CSV file where results will be saved.

        Returns:
        None
        """
        try:
            logger.info("Reading input data...")
            df = self.file_handler.read_json(input_path)

            # Check if the DataFrame is empty after reading
            if df.count() == 0:
                logger.error("No data found in the input file. Exiting process.")
                return

            logger.info("Calculating word frequencies...")
            # Get word counts across all petitions using both abstract and label
            word_counts = self.text_processor.get_word_counts(
                df,
                ['abstract', 'label']
            )

            # Check if word_counts DataFrame is empty
            if word_counts.count() == 0:
                logger.warning("No words found with 5 or more letters in the input data.")
            # Get top 20 words
            top_words = self.text_processor.get_top_n_words(word_counts, n=20)

            logger.info(f"Top 20 words: {top_words}")

            # Calculate word frequencies of top 20 most common words for each petition
            result_df = self.text_processor.calculate_word_frequencies(
                df,
                ['abstract', 'label'],
                top_words # Words to calculate frequencies for
            )

            logger.info("Writing output to CSV...")
            self.file_handler.write_csv(result_df, output_path)

            logger.info("Processing completed successfully!")

        except Exception as e:
            logger.error(f"Error processing petitions: {str(e)}")
            raise

def create_spark_session():
    """
    Create a Spark session.

    Returns:
    SparkSession: A Spark session instance.
    """
    return (SparkSession.builder
            .appName("PetitionProcessor")
            .getOrCreate())

if __name__ == "__main__":
    """
    Main method to process petitions
    """
    spark = create_spark_session()
    transformer = PetitionTransformer(spark)
    transformer.process_petitions(
        "data/input/input_data.json",
        "data/output/petition_word_counts"
    )
