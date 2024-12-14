# Copyright 2024
# Author: Usamah Zaheer

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, split, lower, col, length, count, concat_ws

class TextProcessor:
    @staticmethod
    def get_word_counts(df: DataFrame, text_columns: List[str]) -> DataFrame:
        """
        Process multiple text columns to get word counts for words with 5 or more letters.

        Parameters:
        df (DataFrame): The input DataFrame containing the text data.
        text_columns (List[str]): A list of column names to process.

        Returns:
        DataFrame: A DataFrame with the word counts for words with 5 or more letters.
        """
        # Create a combined text column from all specified columns
        combined_text = concat_ws(' ', *[col(column) for column in text_columns])
        
        return (df
                .select(explode(split(lower(combined_text), r'\W+')).alias('word'))
                .where(length('word') >= 5)
                .groupBy('word')
                .agg(count('*').alias('count'))
                .orderBy('count', ascending=False))

    @staticmethod
    def get_top_n_words(df: DataFrame, n: int = 20) -> List[str]:
        """
        Get the top N most common words from the DataFrame.

        Parameters:
        df (DataFrame): The input DataFrame containing word counts.
        n (int): The number of top words to return (default is 20).

        Returns:
        List[str]: A list of the top N most common words.
        """
        return [row.word for row in df.limit(n).collect()]

    @staticmethod
    def calculate_word_frequencies(
        df: DataFrame, 
        text_columns: List[str],
        top_words: List[str]
    ) -> DataFrame:
        """
        Calculate word frequencies for each petition across multiple text columns.

        Parameters:
        df (DataFrame): The input DataFrame containing the text data.
        text_columns (List[str]): A list of column names to process.
        top_words (List[str]): A list of words to filter and count.

        Returns:
        DataFrame: A DataFrame with word frequencies for each petition.
        """
        # Combine text from all columns
        combined_text = concat_ws(' ', *[col(column) for column in text_columns])

        # Split text into words and convert to lowercase
        words_df = df.select(
            'petition_id',
            explode(split(lower(combined_text), r'\W+')).alias('word')
        )
        # Filter for only the words we care about and count them
        result = (words_df
                  .filter(col('word').isin(top_words))
                  .groupBy('petition_id', 'word')
                  .agg(count('*').alias('count'))
                  .groupBy('petition_id')
                  .pivot('word')
                  .sum('count')
                  .fillna(0))
        return result
