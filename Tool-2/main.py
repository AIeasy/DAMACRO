
import warnings
from multiprocessing import Process, Queue, Event
import pandas as pd
from classify import classify_chunk
import joblib
import lz4.frame
import gzip
import zstandard as zstd
import time
import os
from transfer import transfer_file_via_ssh
def get_folder_size(folder_path):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)

    return total_size / (1024 * 1024)

  
def calculate_throughput(
        data_size: float,
        classification_time: float,
        compression_time: float,
        compression_ratio: float,
        network_speed: float,
):
    total_time = classification_time + compression_time
    class_throughput = data_size / total_time
    netwrok_throughput = network_speed * compression_ratio
    throughput = min(class_throughput, netwrok_throughput)
    return throughput
def compress(data,compression_algorithm,s_path,worker_id):
    #sort the tabel here?
    # drop the label column
    data = data.drop(columns=['label'])
    start_time = time.time()
    if compression_algorithm == 'lz4':
        compressed_data = lz4.frame.compress(data.to_csv(index=False).encode())
    elif compression_algorithm == 'gzip':
        compressed_data = gzip.compress(data.to_csv(index=False).encode())
    elif compression_algorithm == 'zstd':
        cctx = zstd.ZstdCompressor(level=3)
        compressed_data = cctx.compress(data.to_csv(index=False).encode())
    else:
        raise ValueError(f"Unsupported compression algorithm: {compression_algorithm}")
    end_time = time.time()
    compress_time = end_time - start_time
    #print(f"compress time for {worker_id} is {end_time-start_time}")
    save_folder = os.path.join(s_path, compression_algorithm)
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    output_name = f'worker_{worker_id}_compressed.{compression_algorithm}'
    output_path = os.path.join(save_folder, output_name)
    with open(output_path, 'ab') as f:
        f.write(compressed_data)
    return compressed_data,compress_time,output_path
def worker_process(worker_id, input_queue, output_queue,alg,path):
    total_compress_time = 0
    total_transfer_time = 0
    accumulated_data = pd.DataFrame()
    while True:
        data = input_queue.get()
        if data is None:  # Shutdown signal

            if not accumulated_data.empty:
                compressed_data,compress_time,save_folder = compress(accumulated_data,alg,path,worker_id)
                transfer_time = transfer(save_folder)
                total_compress_time += compress_time
                total_transfer_time += transfer_time
                output_queue.put((worker_id, total_compress_time, total_transfer_time))
                break
            break
        
        accumulated_data = pd.concat([accumulated_data, data])
        if len(accumulated_data) >= 10000:

            compressed_data,compress_time,save_folder = compress(accumulated_data,alg,path,worker_id)
            transfer_time = transfer(save_folder)
            total_compress_time += compress_time
            total_transfer_time += transfer_time
            output_queue.put((worker_id, total_compress_time, total_transfer_time))
            accumulated_data = pd.DataFrame()
            
def classify_data(input_queue, output_queue,model):
    start_time = time.time()
    while True:
        chunk = input_queue.get()
        if chunk is None:
            end_time = time.time()
            print(f"Classification Time: {end_time-start_time}")
            output_queue.put((None,end_time-start_time))
            break

        # Classify the chunk and create labeled data
        # ...

        labeled_data = classify_chunk(chunk,model)

        output_queue.put(labeled_data)
    print("classify module is done")
def compress_module(input_queue, output_queue,alg,path):
    start_time = time.time()
    worker_queues = {label: Queue() for label in range(10)}  # Assuming labels are 0-9
    workers = [Process(target=worker_process, args=(label, worker_queues[label], output_queue,alg,path)) for label in range(10)]

    for worker in workers:
        worker.start()

    while True:
        labeled_chunk = input_queue.get()

        if len(labeled_chunk) == 2:  # Shutdown signal
            _,classify_time = labeled_chunk
            for q in worker_queues.values():        
                q.put(None)
            break

        # Split the labeled_chunk DataFrame into clusters based on labels


        for label in range(10):
            cluster_data =labeled_chunk.loc[labeled_chunk['label'] == label]
            worker_queues[label].put(cluster_data)
    total_compress_time = 0
    total_transfer_time = 0
    while not output_queue.empty():
        _, compress_time, transfer_time = output_queue.get()
        total_compress_time += compress_time
        total_transfer_time += transfer_time
    for worker in workers:
        worker.join()
    end_time = time.time()
    moudle_time = end_time-start_time
    print(f"Unparallel Total Compression Time: {total_compress_time}")
    print(f"Total Transfer Time: {total_transfer_time}")
    output_queue.put((moudle_time, total_transfer_time,classify_time,None))
def transfer(path):
    transfer_time = transfer_file_via_ssh(path,"sean", "192.168.204.128", "/home/sean/Desktop/")
    return transfer_time


def main():
    print("process start")
    original_data_size=151.3
    file_path = 'data/original'
    file_name = 'econbiz'
    train_percent = 20
    model_path = 'models'
    model_name = 'AdaBoost'
    alg = 'gzip'
    networkspeed = 5
    compress_save_path = f'data/compressed_data/{train_percent}%_train/{model_name}_{file_name}'
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    model = joblib.load(f'{model_path}/{train_percent}%_train/{model_name}_{file_name}.joblib')
    # Create and start processes
    classify_process = Process(target=classify_data, args=(classify_queue, compress_queue,model))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,alg,compress_save_path))
    start_time = time.time()
    classify_process.start()
    compress_process.start()
    # Stream chunks to classify module
    chunk_size = 10000
    print("loading data stream")
    for  i,chunk in enumerate(pd.read_csv(f'{file_path}/{file_name}.csv', chunksize=chunk_size, delimiter='|')):
        classify_queue.put(chunk)
    print("stream loaded")
    classify_queue.put(None)  # End of data signal

    # Join processes
    print("start join")
    print("classify start")
    classify_process.join()
    print("classify finish")
    print("compress start")

    compress_process.join()
    end_time = time.time()
    print("process time: ",end_time-start_time)
    while transfer_queue.empty() != True:

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
    print(f'Compress module time: {total_compress_time}')
    print(f'Classify module time: {classify_time}')
    file_folder = os.path.join(compress_save_path,alg)
    #print(file_folder)
    compressed_size = get_folder_size(file_folder)
    #print(compressed_size)
    total_compression_ratio = original_data_size / compressed_size
    #print(total_compression_ratio)
    throughput = calculate_throughput(original_data_size,classify_time,total_compress_time,total_compression_ratio,networkspeed)
    print("throughput: ",throughput)
if __name__ == "__main__":
    main()