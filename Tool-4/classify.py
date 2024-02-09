
import warnings
import numpy as np
import json
from sklearn.preprocessing import LabelEncoder
import joblib
def fd_json_read(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    cat_col = config["column_cat_col"]
    col_drop = config["column_drop"]

    return cat_col, col_drop

def preprocess_data_fd(df,cat_col,drop_col):
    le = LabelEncoder()
    for index in cat_col:
        if index < len(df.columns):
            column_name = df.columns[index]
            df[column_name] = le.fit_transform(df[column_name])
        else:
            print(f"Index {index} is out of bounds for the DataFrame.")
    
    df = df.drop(df.columns[drop_col], axis=1)
    df.replace('', np.nan, inplace=True)
    df = df.fillna("0")
    return df
def preprocess_data(df):
    numeric_columns = df.select_dtypes(include='number')
    return numeric_columns
def classify_data(df, model,scaler_path):

    scaler = joblib.load(scaler_path)
    df_scaled = scaler.transform(df)
    predictions = model.predict(df_scaled)
    return predictions
def add_labels_to_chunk(chunk, labels):
    chunk['label'] = labels
    return chunk
def classify_chunk(chunk,model):
    #warnings.filterwarnings("ignore")
    processed_data = preprocess_data(chunk)
    predictions = classify_data(processed_data, model)
    chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels
def classify_chunk_fd(chunk,model,json_path,scaler_path):
    warnings.filterwarnings("ignore")
    cat_col,drop_col = fd_json_read(json_path)
    processed_data = preprocess_data_fd(chunk,cat_col,drop_col)
    #print("out preprocess")
    predictions = classify_data(processed_data, model,scaler_path)
    chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels
def classify_chunk_base(chunk,label):
    labels = [random.randint(0, 9) for _ in range(chunk.shape[0])]
    chunk_with_labels = add_labels_to_chunk(chunk,labels)
    return chunk_with_labels
