import pandas as pd
import lz4.frame
import gzip
import zstandard as zstd
import io
import json

'''
def get_compression_sizes(df):
    
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    data = buffer.getvalue()

    # Compress using zstandard
    zstd_compressor = zstd.ZstdCompressor()
    zstd_compressed = zstd_compressor.compress(data)
    zstd_size = len(zstd_compressed)

    # Compress using lz4
    lz4_compressed = lz4.frame.compress(data)
    lz4_size = len(lz4_compressed)

    # Compress using gzip
    gzip_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as f:
        f.write(data)
    gzip_compressed = gzip_buffer.getvalue()
    gzip_size = len(gzip_compressed)

    return {"zstandard": zstd_size, "lz4": lz4_size, "gzip": gzip_size}


def read_json_file(file_name):
    try:
        with open(file_name, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def write_json_file(file_name, data):
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=4)

def compress_with_FD_Linear(df, FD_list, save_file_name, FD_type):
    df_sorted = df.sort_values(by=FD_list)
    sorted_column_compression = get_compression_sizes(df_sorted)
    results = {'sorted_by_columns_' + FD_type: sorted_column_compression}

    existing_data = read_json_file(save_file_name)
    existing_data.update(results)
    write_json_file(save_file_name, existing_data)

    return results

def compress_original_Linear(df, save_file_name):
    original = get_compression_sizes(df)
    results = {'original': original}

    existing_data = read_json_file(save_file_name)
    existing_data.update(results)
    write_json_file(save_file_name, existing_data)

    return results

def compress_sortall_Linear(df, save_file_name):
    df_sortall = df.sort_values(by=df.columns.tolist())
    sort_all = get_compression_sizes(df_sortall)
    results = {'sortall': sort_all}

    existing_data = read_json_file(save_file_name)
    existing_data.update(results)
    write_json_file(save_file_name, existing_data)

    return results


'''

def convert_columns_to_string(df):
    """
    Convert all columns of type 'object' to string to avoid data type issues.
    """
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
    return df

def get_compression_sizes(df):
    df = convert_columns_to_string(df)  # Ensure consistent data types

    buffer = io.BytesIO()
    df.to_parquet(buffer)
    data = buffer.getvalue()

    # Compress using zstandard
    zstd_compressor = zstd.ZstdCompressor()
    zstd_compressed = zstd_compressor.compress(data)
    zstd_size = len(zstd_compressed)

    # Compress using lz4
    lz4_compressed = lz4.frame.compress(data)
    lz4_size = len(lz4_compressed)

    # Compress using gzip
    gzip_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as f:
        f.write(data)
    gzip_compressed = gzip_buffer.getvalue()
    gzip_size = len(gzip_compressed)

    return {"zstandard": zstd_size, "lz4": lz4_size, "gzip": gzip_size}

def read_json_file(file_name):
    try:
        with open(file_name, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def write_json_file(file_name, data):
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=4)

def compress_with_FD_Linear(df, FD_list, save_file_name, FD_type):
    df_sorted = df.sort_values(by=FD_list)
    sorted_column_compression = get_compression_sizes(df_sorted)
    results = {'sorted_by_columns_' + FD_type: sorted_column_compression}

    existing_data = read_json_file(save_file_name)
    existing_data.update(results)
    write_json_file(save_file_name, existing_data)

    return results

def compress_original_Linear(df, save_file_name):
    original = get_compression_sizes(df)
    results = {'original': original}

    existing_data = read_json_file(save_file_name)
    existing_data.update(results)
    write_json_file(save_file_name, existing_data)

    return results

def compress_sortall_Linear(df, save_file_name):
    df_sortall = df.sort_values(by=df.columns.tolist())
    sort_all = get_compression_sizes(df_sortall)
    results = {'sortall': sort_all}

    existing_data = read_json_file(save_file_name)
    existing_data.update(results)
    write_json_file(save_file_name, existing_data)

    return results