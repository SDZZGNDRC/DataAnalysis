import pandas as pd
import numpy as np
import pytest

from .aggregate_parquet import split_dataframe  # Replace 'your_module' with the actual module name

def test_split_dataframe():
    # Create a sample dataframe
    df = pd.DataFrame({
        'timestamp': [1, 2, 2, 5, 6, 8, 10, 11, 12],
        'value': [10, 20, 30, 40, 50, 60, 70, 80, 90]
    })

    # Expected output
    expected_chunks = [
        pd.DataFrame({'timestamp': [1, 2, 2], 'value': [10, 20, 30]}),
        pd.DataFrame({'timestamp': [5, 6], 'value': [40, 50]}),
        pd.DataFrame({'timestamp': [8, 10], 'value': [60, 70]}),
        pd.DataFrame({'timestamp': [11, 12], 'value': [80, 90]})
    ]

    # Call the function
    chunks = split_dataframe(df, chunk_size=2)

    # Compare the actual and expected outputs
    assert len(chunks) == len(expected_chunks)  # Check the number of chunks

    for expected_chunk, actual_chunk in zip(expected_chunks, chunks):
        print(expected_chunk.reset_index(drop=True))
        print(actual_chunk.reset_index(drop=True))
        assert expected_chunk.reset_index(drop=True).equals(actual_chunk.reset_index(drop=True))  # Check if the chunks are equal

    # Additional test case with an empty dataframe
    empty_df = pd.DataFrame({'timestamp': [], 'value': []})
    empty_chunks = split_dataframe(empty_df, chunk_size=2)
    assert len(empty_chunks) == 0  # Check that no chunks are returned for an empty dataframe

    # Additional test case with a dataframe containing a single row
    single_row_df = pd.DataFrame({'timestamp': [1], 'value': [10]})
    single_row_chunks = split_dataframe(single_row_df, chunk_size=2)
    assert len(single_row_chunks) == 1  # Check that a single chunk is returned for a single-row dataframe

    # Additional test case with a dataframe where all timestamps are the same
    same_timestamp_df = pd.DataFrame({'timestamp': [1, 1, 1], 'value': [10, 20, 30]})
    same_timestamp_chunks = split_dataframe(same_timestamp_df, chunk_size=2)
    assert len(same_timestamp_chunks) == 1  # Check that a single chunk is returned for a dataframe with same timestamps

    # Additional test case with a large dataframe
    large_df = pd.DataFrame({'timestamp': range(10000), 'value': range(10000)})
    large_chunks = split_dataframe(large_df, chunk_size=100)
    assert len(large_chunks) == 100  # Check the number of chunks for a large dataframe

    # Add more test cases as needed

# Run the tests
pytest.main()