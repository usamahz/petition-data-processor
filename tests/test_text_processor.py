# Copyright 2024
# Author: Usamah Zaheer

import pytest
from pyspark.sql import SparkSession
from src.data_processing.text_processor import TextProcessor
from src.utils.file_handlers import FileHandler

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("Test") \
        .getOrCreate()
    return spark_session

def test_get_word_counts(spark):
    """
    Test the get_word_counts method of the TextProcessor class.

    This test verifies that the word counts are returned correctly
    and that known words are present in the results.
    """
    df = FileHandler(spark=spark)
    df = df.read_json(input_path='tests/test_data/test_input_data.json')

    word_counts = TextProcessor.get_word_counts(df, ["abstract", "label"])

    # Check if the word counts are returned
    assert word_counts.count() > 0
    # Check if known words are present
    assert "debates" in [row.word for row in word_counts.collect()]
    assert "three" in [row.word for row in word_counts.collect()]
    assert "attend" in [row.word for row in word_counts.collect()]

def test_get_top_n_words(spark):
    """
    Test the get_top_n_words method of the TextProcessor class.

    This test checks that the top N words are returned correctly
    and that known top words are included in the results.
    """
    df = FileHandler(spark=spark)
    df = df.read_json(input_path='tests/test_data/test_input_data.json')

    word_counts = TextProcessor.get_word_counts(df, ["abstract", "label"])
    top_words = TextProcessor.get_top_n_words(word_counts, n=5)
    
    # Check if the top 5 words are returned
    assert len(top_words) <= 5
    # Check if known top words are present
    assert "attend" in top_words
    assert "commons" in top_words
    assert "reform" in top_words

def test_calculate_word_frequencies(spark):
    """
    Test the calculate_word_frequencies method of the TextProcessor class.

    This test ensures that the word frequencies for the top words
    are calculated correctly for each petition.
    """
    df = FileHandler(spark=spark)
    df = df.read_json(input_path='tests/test_data/test_input_data.json')

    word_counts = TextProcessor.get_word_counts(df, ["abstract", "label"])
    top_words = TextProcessor.get_top_n_words(word_counts, n=5)
    frequencies_df = TextProcessor.calculate_word_frequencies(df, ["abstract", "label"], top_words)

    # Ground truth values for the top 5 words
    expected_values = {
        'attend': 1,
        'commons': 2,
        'reform': 1,
        'suggest': 1,
        'three': 2
    }
    
    # Check if the word frequencies are correct
    for word in top_words:
        actual_values = [row[word] for row in frequencies_df.select(word).collect()][-1]
        assert actual_values == expected_values[word]