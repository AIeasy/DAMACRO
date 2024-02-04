
import warnings
def preprocess_data(df):
    numeric_columns = df.select_dtypes(include='number')
    return numeric_columns
def classify_data(df, model):
    predictions = model.predict(df)
    return predictions
def add_labels_to_chunk(chunk, labels):
    chunk['label'] = labels
    return chunk
def classify_chunk(chunk,model):
    warnings.filterwarnings("ignore")
    processed_data = preprocess_data(chunk)
    predictions = classify_data(processed_data, model)
    chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels
def classify_chunk_base(chunk,label):
    labels = [label]* chunk.shape[0]
    chunk_with_labels = add_labels_to_chunk(chunk,labels)
    return chunk_with_labels
