from multiprocessing import Process, Queue, Event
import time
from classify import classify_chunk,classify_chunk_base,classify_chunk_scaler_kp,classify_chunk_scaler_km,classify_chunk_km,classify_chunk_kp,incremental_k_prototypes,add_labels_to_chunk,classify_chunk_base_one
import joblib
import pandas as pd
import os
from transfer import transfer_file_via_ssh,set_network_conditions,reset_network_conditions
import lz4.frame
import gzip
import zstandard as zstd
import math
import csv
import subprocess
from preprocess_data import read_data
from sklearn.preprocessing import StandardScaler
import numpy as np
import gc
'''

def calculate_cost(
        total_time,
        compression_time: float,
        compression_ratio: float,
        original_size: float,
        num_cores: int,
        cost_scale = 'TB', # calculate the cost of handreds of TBs
        p_cpu = 0.048,#$/hour?
        p_net = 0.05,#$/GB?
):
    #print(total_time,compression_ratio)
    #((((0.048 / 3600) * 11) * 1.0854 + ((156.11 / 3.29) / 1024) * 0.05) / 156.11) * (1024*1024)
    base_cost = ((p_cpu / 3600) * num_cores) * total_time + ((original_size / compression_ratio) / 1024) * p_net 
    cost_scale = 1024 * 1024
    cost = (base_cost / original_size) * cost_scale 
    return cost
    '''
def calculate_cost(
        classify_time:float,
        compression_times: dict,
        compression_ratio: float,
        original_size: float,
        num_cores: int,
        cost_scale = 'TB', # calculate the cost of handreds of TBs
        p_cpu = 0.048,#$/hour?
        p_net = 0.05,#$/GB?
):
    
    #print(total_time,compression_ratio)
    #((((0.048 / 3600) * 11) * 0.1552 + ((113.47 / 4.6553) / 1024) * 0.05) / 113.47) * (1024*1024)
    ((0.048 / 3600) * 11) * 0.1552+((0.048 / 3600) *27.4252)
    comp_cost = 0
    class_cost  =0
    for worker_id, compression_time in compression_times.items():
        comp_cost += (p_cpu / 3600) * compression_time
    class_cost = (p_cpu / 3600) * classify_time
    base_cost = comp_cost+class_cost + ((original_size / compression_ratio) / 1024) * p_net 
    cost_scale = 1024 * 1024
    cost = (base_cost / original_size) * cost_scale 
    return cost
def get_folder_size(folder_path):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            #print("file path:",file_path)
            #print('file size:',os.path.getsize(file_path))
            total_size += os.path.getsize(file_path)
            os.remove(file_path)

    return total_size / (1024 * 1024)
def calculate_throughput(
        data_size: float,
        classification_time: float,
        compression_time: float,
        compression_ratio: float,
        network_speed: float,
):
    if classification_time != 0:

        total_time = classification_time + compression_time
        class_throughput = data_size /classification_time 
        compress_thorughput = data_size/compression_time
        netwrok_throughput = network_speed 
        print("class:" ,class_throughput)
        print("net:" ,netwrok_throughput)
        print("compress:" ,compress_thorughput)
        throughput = min(class_throughput, netwrok_throughput,compress_thorughput)
    else:
        compress_thorughput = data_size/compression_time
        netwrok_throughput = network_speed 
        print("net:" ,netwrok_throughput)
        print("compress:" ,compress_thorughput)
        throughput = min(compress_thorughput, netwrok_throughput)
    return throughput

def compress_with_FD(data, compression_algorithm, FD_list = ["split", "rating", "model_attr"]):
    # Drop the label column and other pre-processing
    data = data.drop(columns=['label'])
    start_time = time.time()
    data = data.sort_values(by=FD_list)
    compressed_columns = {}
    i=0
    for column in data.columns:  
    # Compress data based on the specified algorithm
        column_data = data[column].tolist()
        column_data.insert(0,column)
        column_data = pd.DataFrame(column_data)
        if compression_algorithm == 'lz4':
            compressed_data = lz4.frame.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'gzip':
            compressed_data = gzip.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'zstd':
            cctx = zstd.ZstdCompressor(level=3)
            compressed_data = cctx.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        else:
            raise ValueError(f"Unsupported compression algorithm: {compression_algorithm}")
        i+=1
    end_time = time.time()
    compress_time = end_time - start_time

    return compressed_columns, compress_time
def compress(data, compression_algorithm, FD_list = ["split", "rating", "model_attr"]):
    # Drop the label column and other pre-processing
    data = data.drop(columns=['label'])
    start_time = time.time()
    compressed_columns = {}
    i=0
    for column in data.columns:  
    # Compress data based on the specified algorithm
        column_data = data[column].tolist()
        column_data.insert(0,column)
        column_data = pd.DataFrame(column_data)
        if compression_algorithm == 'lz4':
            compressed_data = lz4.frame.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'gzip':
            compressed_data = gzip.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'zstd':
            cctx = zstd.ZstdCompressor(level=3)
            compressed_data = cctx.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        else:
            raise ValueError(f"Unsupported compression algorithm: {compression_algorithm}")
        i+=1
    end_time = time.time()
    compress_time = end_time - start_time

    return compressed_columns, compress_time
def compress_debug(data, compression_algorithm):
    # Drop the label column and other pre-processing
    data = data.drop(columns=['label'])
    start_time = time.time()
    compressed_columns = {}
    i=0
    for column in data.columns:  
    # Compress data based on the specified algorithm
        column_data = data[column].tolist()
        column_data.insert(0,column)
        #print(column_data)
        column_data = list(map(str,column_data))
        #column_data = pd.DataFrame(column_data)
        #print(column_data)
        column_data = "".join(column_data).encode()
        if compression_algorithm == 'lz4':
            compressed_data = lz4.frame.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'gzip':
            compressed_data = gzip.compress(column_data)
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'zstd':
            cctx = zstd.ZstdCompressor(level=3)
            compressed_data = cctx.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        else:
            raise ValueError(f"Unsupported compression algorithm: {compression_algorithm}")
        i+=1
    end_time = time.time()
    compress_time = end_time - start_time

    return compressed_columns, compress_time    
def transfer_process(worker_id, transfer_input_queue,output_queue,compression_algorithm,s_path, remote_username, remote_host, remote_file_path,network_speed):
    network_speed = math.ceil(network_speed/10*1024*8)
    print(f'transfer_{worker_id}_start\n')
    save_folder = os.path.join(s_path, compression_algorithm)#create folder
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    process_transfer_time =0
    while True:
        compress_tuple = transfer_input_queue.get()
        
        if len(compress_tuple) == 4:  # Shutdown signal,now the compression is done, we can start transfer

            with open(f'results/debug.txt', "a") as debug_file:
                debug_file.write(f'transfer_{worker_id}_recived\n')
            print(f'transfer_{worker_id}_recived\n')
            _,worker_id, total_compress_time,compressed_dic = compress_tuple
            if len(compressed_dic)!=0:
                file_size =0
                for key in compressed_dic:
                    output_name = f'worker_{worker_id}_{key}_compressed.{compression_algorithm}'#get file name
                    output_path = os.path.join(save_folder, output_name)#get save path
                    compressed_data = compressed_dic[key]#get the column data
                    with open(output_path, 'ab') as f:
                        f.write(compressed_data)#create a compressed file for that column

                start_transfer_time = time.time()
                for filename in os.listdir(save_folder):#loop throgh all columns files saved in folder
                    file_path = os.path.join(save_folder, filename)
                    if (f'worker_{worker_id}_' in filename):
                        file = file_path
                        
                        #transfer_file_via_ssh(local_file_path=output_path, remote_username=remote_username, remote_host=remote_host, remote_file_path=remote_file_path)#transfer it
                        file_size += os.path.getsize(file)
                        cmd = ["scp", '-l',f'{network_speed}',file_path, f"{remote_username}@{remote_host}:{remote_file_path}"]
                        subprocess.run(cmd)
                        
                        print(f'transfer_{worker_id} done sending last pice\n')
                end_transfer_time = time.time()
                process_transfer_time += end_transfer_time - start_transfer_time

                #print('debugging')
                output_queue.put((None,total_compress_time,process_transfer_time))#transfer is finished
                
                with open(f'results/debug.txt', "a") as result_file:
                    result_file.write(f'transfer_{worker_id}_stopped_l\n')
                print(f'transfer_{worker_id}_stopped_l\n')
                with open(f'results/trans_time.txt', "a") as t_file:
                    t_file.write(f'Transfer_{worker_id}_Transfertime: {process_transfer_time:.4f}\n')
                    #t_file.write(f'Transfer_{worker_id}_TransferSize: {file_size / (1024 * 1024):.4f}\n')
                break
            else:
                start_transfer_time = time.time()
                for filename in os.listdir(save_folder):#loop throgh all columns files saved in folder
                    file_path = os.path.join(save_folder, filename)
                    #print(filename)
                    #print(worker_id)
                    if (f'worker_{worker_id}' in filename):
                        #transfer_file_via_ssh(local_file_path=output_path, remote_username=remote_username, remote_host=remote_host, remote_file_path=remote_file_path)#transfer it
                        #print("transfering: ",filename)
                        '''
                        debug = os.path.join(save_folder,f'transfering_{filename}.txt')
                        with open(debug,'ab') as gg:
                            gg.write('1')
                        '''
                        cmd = ["scp", '-l',f'{network_speed}',file_path, f"{remote_username}@{remote_host}:{remote_file_path}"]
                        subprocess.run(cmd)
                        print(f'transfer_{worker_id} done sending \n')
                end_transfer_time = time.time()
                process_transfer_time += end_transfer_time - start_transfer_time
                output_queue.put((None,total_compress_time,process_transfer_time))
                with open(f'results/debug.txt', "a") as result_file:
                    result_file.write(f'transfer_{worker_id}_stopped\n')
                with open(f'results/trans_time.txt', "a") as t_file:
                    t_file.write(f'Transfer_{worker_id}_Transfertime: {process_transfer_time:.4f}\n')
                print(f'transfer_{worker_id}_stopped\n')
                break
        else:
            #update the compressed file with new chunks
            worker_id, total_compress_time,compressed_dic = compress_tuple
            for key in compressed_dic:
                output_name = f'worker_{worker_id}_{key}_compressed.{compression_algorithm}'#get file name
                output_path = os.path.join(save_folder, output_name)#get save path
                compressed_data = compressed_dic[key]#get the column data
                with open(output_path, 'ab') as f:
                    f.write(compressed_data)#create a compressed file for that column
def worker_process(worker_id, input_queue, output_queue,alg,path):
    print(f'worker_{worker_id}_start\n')
    total_compress_time = 0
    accumulated_data = pd.DataFrame()
    call = 0

    while True:
        data = input_queue.get()

        if data is None:  # Shutdown  signal
            if worker_id == 12:
                with open(f'results/debug.txt', "a") as debug_file:
                    debug_file.write(f'aaaaaaaaa\n')
            if not accumulated_data.empty:
                compressed_data,compress_time = compress(accumulated_data,alg)#get leftover compressed_dic
                call+=1
                #print(f'debugworkerl: {worker_id}',np.shape(accumulated_data),compress_time)
                total_compress_time += compress_time
                output_queue.put((None,worker_id, total_compress_time,compressed_data))#feed the leftover compressed_dic into transfer and tell transfer to stop
                print(f'worker_{worker_id}_end_l\n')
                with open(f'results/time.txt', "a") as result_file:
                    result_file.write(f"Worker_{worker_id}_Compressiontime: {total_compress_time:.4f}\n")
                    #result_file.write(f"Worker_{worker_id}_call_count: {call}\n")
                with open(f'results/count.txt', "a") as countresult_file:
                    countresult_file.write(f"Worker_{worker_id}_call_count: {call}\n")
                with open(f'results/size.txt', "a") as size_file:
                    size_file.write(f"Worker_{worker_id}_left_size: {np.shape(accumulated_data)}, time: {compress_time}\n")
                if worker_id == 12:
                    with open(f'results/debug.txt', "a") as debug_file:
                        debug_file.write(f'worker_{worker_id}_end_l\n')
                print(f"worker_{worker_id} call count: {call}\n")
                break
                
            output_queue.put((None,worker_id, total_compress_time,""))#feed the leftover compressed_dic into transfer and tell transfer to stop
            with open(f'results/time.txt', "a") as result_file:
                    result_file.write(f"Worker_{worker_id}_Compressiontime: {total_compress_time:.4f}\n")
            with open(f'results/count.txt', "a") as countresult_file:
                
                countresult_file.write(f"Worker_{worker_id}_call_count: {call}\n")
            with open(f'results/size.txt', "a") as size_file:
                    size_file.write(f"Worker_{worker_id}_size: {np.shape(accumulated_data)}\n")
            print(f'worker_{worker_id}_end\n')
            if worker_id == 12:
                    with open(f'results/debug.txt', "a") as debug_file:
                        debug_file.write(f'worker_{worker_id}_end\n')
            break
        
        accumulated_data = pd.concat([accumulated_data, data])
        if len(accumulated_data) >= 10000:

            compressed_data,compress_time = compress(accumulated_data,alg)#get compressed_dic
            call+=1
            with open(f'results/count.txt', "a") as countresult_file:
                
                countresult_file.write(f"Worker_{worker_id}_call_count: {call}\n")
            with open(f'results/size.txt', "a") as size_file:
                    size_file.write(f"Worker_{worker_id}_size: {np.shape(accumulated_data)}, time: {compress_time}\n")
            #print(f'worker_{worker_id}_end\n')
            #print(f'debugworker: {worker_id}',np.shape(accumulated_data),compress_time)
            total_compress_time += compress_time
            output_queue.put((worker_id, total_compress_time,compressed_data))#feed the compressed_dic into transfer
            accumulated_data = pd.DataFrame()
def worker_process_sig(input_queue, output_queue,alg,path):
    total_compress_time = 0
    accumulated_data = pd.DataFrame()
    call = 0
    while True:
        data,worker_id = input_queue.get()

        if data is None:  # Shutdown  signal
            if not accumulated_data.empty:
                compressed_data,compress_time = compress(accumulated_data,alg)#get leftover compressed_dic
                call+=1
                print(f'debugworkerl:',np.shape(accumulated_data),compress_time)
                total_compress_time += compress_time
                #output_queue.put((None, total_compress_time,compressed_data))#feed the leftover compressed_dic into transfer and tell transfer to stop
                print(f'worker__end_l\n')
                with open(f'results/time.txt', "a") as result_file:
                    result_file.write(f"Worker_Compressiontime: {total_compress_time:.4f}\n")
                    result_file.write(f"Worker__call_count: {call}\n")
                with open(f'results/count.txt', "a") as countresult_file:
                    countresult_file.write(f"Worker__call_count: {call}\n")
                print(f"worker_ call count: {call}\n")
                break
                
            #output_queue.put((None, total_compress_time,""))#feed the leftover compressed_dic into transfer and tell transfer to stop
            with open(f'results/time.txt', "a") as result_file:
                    result_file.write(f"Worker_Compressiontime: {total_compress_time:.4f}\n")
            with open(f'results/count.txt', "a") as countresult_file:
                
                countresult_file.write(f"Worker_call_count: {call}\n")
            print(f'worker_end\n')
            break
        accumulated_data = pd.concat([accumulated_data, data])
        compressed_data,compress_time = compress(accumulated_data,alg)#get compressed_dic
        call+=1
        #print(worker_id)
        #print(f'debugworker: {worker_id}',np.shape(accumulated_data),compress_time)
        total_compress_time += compress_time
        #output_queue.put((worker_id, total_compress_time,compressed_data))#feed the compressed_dic into transfer
        accumulated_data = pd.DataFrame()

def classify_module(input_queue, output_queue,model,scaler_path):
    total_time = 0
    while True:
        obj= input_queue.get()
        if obj is not None:
            chunk = obj[0]
            chunk_og = obj[1]
        if obj is None:

            print(f"Classification Time: {total_time}")
            output_queue.put((None,total_time))
            break
        labeled_data,class_time = classify_chunk(chunk,chunk_og,model)
        #print(labeled_data)
        total_time += class_time
        output_queue.put(labeled_data)
    print("classify module is done")
    return
def classify_module_scaler_kp(input_queue, output_queue,model,scaler_path,filename,num_i,cat_i,model_name):
    #print(scaler_path)
    total_time = 0
    while True:
        obj= input_queue.get()
        if obj is not None:
            chunk = obj
        if obj is None:
            print(f"Classification Time: {total_time}")
            output_queue.put((None,total_time))
            break
        if  model_name not in ['DecisionTree','GaussianNB']:
            labeled_data,class_time = classify_chunk_scaler_kp(chunk,filename,model,scaler_path,num_i,cat_i)
        else:
            labeled_data,class_time = classify_chunk_kp(chunk,filename,model,num_i,cat_i)
        #print(labeled_data)
        total_time += class_time
        output_queue.put(labeled_data)
    print("classify module is done")
    return
def classify_module_scaler_km(input_queue, output_queue,model,scaler_path,model_name):
    #print(scaler_path)
    total_time = 0
    while True:
        obj= input_queue.get()
        if obj is not None:
            chunk = obj
        if obj is None:
            print(f"Classification Time: {total_time}")
            output_queue.put((None,total_time))
            break
        if  model_name not in ['DecisionTree','GaussianNB']:
            labeled_data,class_time = classify_chunk_scaler_km(chunk,model,scaler_path)
        else:
            labeled_data,class_time = classify_chunk_km(chunk,model)
        total_time += class_time
        output_queue.put(labeled_data)
    print("classify module is done")

    return
def classify_module_base(input_queue, output_queue):
    
    i = 0 
    while True:
        if i >9:
            i=0
        chunk = input_queue.get()
        if chunk is None:
            output_queue.put((None,0))
            break
        labeled_data = classify_chunk_base_one(chunk)
        output_queue.put(labeled_data)
        i += 1
    print("classify module is done")
    return
def classify_module_online(input_queue, output_queue,num_i,cat_i):
    i = 0
    centroids = []
    total_time =0
    while True:
        chunk = input_queue.get()
        if chunk is None:
            print(f"Classification Time: {total_time}")
            output_queue.put((None,total_time))
            break
        else:  
            if i ==0:
                new_cat_index = np.arange(len(cat_i))
                new_num_index = np.arange(len(cat_i), len(cat_i) + len(num_i))
                num = chunk[num_i]
                cat = chunk[cat_i]
                new_chunk = pd.concat([cat,num], axis=1)

                chunk_l = new_chunk.values.tolist()

                start = time.time()
                scaler = StandardScaler()
                chunk_array = np.array(chunk_l)

                temp_num = chunk_array[:, new_num_index]

                num_scaled = scaler.fit_transform(temp_num)
                scaled_chunk = np.hstack((chunk_array[:, new_cat_index], num_scaled)).tolist()
                labels, centroids = incremental_k_prototypes(scaled_chunk, 10, new_cat_index, new_num_index, 1, centroids)
                end = time.time()
                total_time += end-start
                labeled_data = add_labels_to_chunk(chunk,labels)
                output_queue.put(labeled_data)
                i=1
            else:
                new_cat_index = np.arange(len(cat_i))
                new_num_index = np.arange(len(cat_i), len(cat_i) + len(num_i))
                num = chunk[num_i]
                cat = chunk[cat_i]
                new_chunk = pd.concat([cat,num], axis=1)

                chunk_l = new_chunk.values.tolist()
                start = time.time()
                scaler = StandardScaler()
                chunk_array = np.array(chunk_l)

                temp_num = chunk_array[:, new_num_index]

                num_scaled = scaler.fit_transform(temp_num)
                #num_scaled = num_scaled.astype(np.float64)
                scaled_chunk = np.hstack((chunk_array[:, new_cat_index], num_scaled)).tolist()
                labels, centroids = incremental_k_prototypes(scaled_chunk, 10, new_cat_index, new_num_index, 0, centroids)
                end = time.time()
                #print(end-start)
                total_time += end-start
                labeled_data = add_labels_to_chunk(chunk,labels)
                output_queue.put(labeled_data)
    print("classify module is done")
    return
def compress_module(input_queue, output_queue,alg,path,num_worker, remote_username, remote_host, remote_file_path,network_speed,name):
    print("in the compress module")

    worker_queues = {label: Queue() for label in range(num_worker)}  # for number of worker, create worker queues
    transfer_queues = {label: Queue() for label in range(num_worker)}#for number of worker, create transfer queues
    workers = [Process(target=worker_process, args=(label, worker_queues[label], transfer_queues[label],alg,path)) for label in range(num_worker)]#create worker process
    transfers = [Process(target=transfer_process, args=(label, transfer_queues[label],output_queue,alg,path, remote_username, remote_host, remote_file_path,network_speed)) for label in range(num_worker)] #create transfer process
    if name == "Submissions":
        g5 = 0
        g7 =0
        g9 =0
        g2 =2
        g6=0
        extra_queue_5 = Queue()
        extra_queue_5_t = Queue()
        extra_worker_5= Process(target=worker_process, args=(11, extra_queue_5, extra_queue_5_t,alg,path))
        extra_transfer_5=Process(target=transfer_process, args=(11, extra_queue_5_t,output_queue,alg,path, remote_username, remote_host, remote_file_path,network_speed))
        extra_queue_7 = Queue()
        extra_queue_7_t = Queue()
        extra_worker_7 = Process(target=worker_process, args=(12, extra_queue_7, extra_queue_7_t,alg,path))
        extra_transfer_7=Process(target=transfer_process, args=(12, extra_queue_7_t,output_queue,alg,path, remote_username, remote_host, remote_file_path,network_speed))
        extra_queue_5_2 = Queue()
        extra_queue_5_2_t = Queue()
        extra_worker_5_2 = Process(target=worker_process, args=(13, extra_queue_5_2, extra_queue_5_2_t,alg,path))
        extra_transfer_5_2=Process(target=transfer_process, args=(13, extra_queue_5_2_t,output_queue,alg,path, remote_username, remote_host, remote_file_path,network_speed))
        extra_queue_7_2 = Queue()
        extra_queue_7_2_t = Queue()
        extra_worker_7_2 = Process(target=worker_process, args=(15, extra_queue_7_2, extra_queue_7_2_t,alg,path))
        extra_transfer_7_2=Process(target=transfer_process, args=(15, extra_queue_7_2_t,output_queue,alg,path, remote_username, remote_host, remote_file_path,network_speed))
        extra_queue_2 = Queue()
        extra_queue_2_t = Queue()
        #extra_worker_2 = Process(target=worker_process, args=(14, extra_queue_2, extra_queue_2_t,alg,path))
        #extra_transfer_2=Process(target=transfer_process, args=(14, extra_queue_2_t,output_queue,alg,path, remote_username, remote_host, remote_file_path,network_speed))
        extra_worker_5.start()
        extra_worker_7.start()
        extra_worker_5_2.start()
        extra_worker_7_2.start()
        extra_transfer_7_2.start()
        extra_transfer_5.start()
        extra_transfer_7.start()
        extra_transfer_5_2.start()

        #extra_worker_2.start()
        #extra_transfer_2.start()
    for worker in workers:#starting worker listening for labeled chunk
        worker.start()
    for transfer in transfers:#start transfer listening for compressed_dic which cotains columns
        transfer.start()
    x=0
    i = 0

    while True:
        #print(x)
        labeled_chunk = input_queue.get()#get the lableed chunk
        #x+=1
        #print("chunk:",np.shape(labeled_chunk))

        if len(labeled_chunk) == 2:  # Shutdown signal
            _,classify_time = labeled_chunk
            print("time to stop")
            for q in worker_queues.values():        
                q.put(None)#stop worker
            if name == "Submissions":
                extra_queue_5.put(None)
                extra_queue_2.put(None)
                extra_queue_7.put(None)
                extra_queue_5_2.put(None)
                extra_queue_7_2.put(None)
            break

        # Split the labeled_chunk DataFrame into clusters based on labels
        if i ==0:
            start_time_c = time.time()
            i+=1
        for label in range(num_worker):
            cluster_data =labeled_chunk.loc[labeled_chunk['label'] == label]
            if name == "Submissions":
                if label == 9:
                    if g9 == 0:
                        worker_queues[label].put(cluster_data)
                        g9 = 1
                    else:
                        #extra_queue_9.put(cluster_data)
                        g9=0
                elif label == 7:
                    if g7 == 0:
                        worker_queues[label].put(cluster_data)
                        g7 = 1
                    elif g7 == 1:
                        extra_queue_7_2.put(cluster_data)
                        g7 =2
                    else:
                        extra_queue_7.put(cluster_data)
                        g7=0
                elif label == 5:
                    if g5 == 0:
                        worker_queues[label].put(cluster_data)
                        g5 = 1
                    elif g5 == 1:
                        extra_queue_5_2.put(cluster_data)
                        g5 =2
                    else:
                        extra_queue_5.put(cluster_data)
                        g5=0
                elif label == 2:
                    if g2 == 0:
                        worker_queues[label].put(cluster_data)
                        g2 = 1
                    else:
                        worker_queues[1].put(cluster_data)
                        g2=0
                elif label == 6:
                    if g6 == 0:
                        worker_queues[label].put(cluster_data)
                        g6 = 1
                    else:
                        worker_queues[0].put(cluster_data)
                        g6=0
                else:
                    worker_queues[label].put(cluster_data)
            #print(f'label {label}',np.shape(cluster_data))
            else:
                worker_queues[label].put(cluster_data)

    total_transfer_time =0
#timing for compression time
    for worker in workers:#wait worker finish it jobs
        worker.join()
    end_time_c = time.time()
    print("Worker is done:D")
    if name == "Submissions":
        extra_worker_7.join()
        #extra_worker_2.join()
        extra_worker_5.join()
        extra_worker_5_2.join()
        extra_worker_7_2.join()
    start_time_t = time.time()#timing for transfer time
    if name == "Submissions":
        extra_transfer_5.join()
        #extra_transfer_2.join()
        extra_transfer_7.join()
        extra_transfer_5_2.join()
        extra_transfer_7_2.join()
    for transfer in transfers:#wait transfer finish it jobs
        transfer.join()

        #extra_transfer_9.join()
    end_time_t = time.time()
    print("Trasfer is done :D")

    compress_time = end_time_c-start_time_c
    total_transfer_time = end_time_t-start_time_t
    print(f"Total Compression Time: {compress_time}")
    print(f"Total Tramsfer Time: {total_transfer_time}")
    output_queue.put((compress_time, total_transfer_time,classify_time,None))
    print("out compress model")
    return
def compress_module_sig(input_queue, output_queue,alg,path,num_worker, remote_username, remote_host, remote_file_path,network_speed):
    print("in the compress module")
    i=0
    worker_queue = Queue()
    transfer_queue = Queue()
    worker = Process(target=worker_process_sig, args=(worker_queue, transfer_queue,alg,path))
    worker.start()

    while True:
        
        labeled_chunk = input_queue.get()#get the lableed chunk
        #print("chunk:",np.shape(labeled_chunk))

        if len(labeled_chunk) == 2:  # Shutdown signal
            _,classify_time = labeled_chunk
            print("time to stop")
            worker_queue.put((None,None))#stop worker
            break

        # Split the labeled_chunk DataFrame into clusters based on labels
        if i ==0:
            start_time_c = time.time()
            i+=1
        for label in range(num_worker):
            cluster_data =labeled_chunk.loc[labeled_chunk['label'] == label]
            #print(f'label {label}',np.shape(cluster_data))
            worker_queue.put((cluster_data,label))

    total_transfer_time =0
    #timing for compression time
    print("22222222222222222222222")

    worker.join()
    print('11111111111111111111111111')
    end_time_c = time.time()
    print("Worker is done:D")
    start_time_t = time.time()#timing for transfer time
    end_time_t = time.time()
    compress_time = end_time_c-start_time_c
    total_transfer_time = end_time_t-start_time_t
    print(f"Total Compression Time: {compress_time}")
    print(f"Total Tramsfer Time: {total_transfer_time}")
    output_queue.put((compress_time, total_transfer_time,classify_time,None))
    print("out compress model")
    return
def expierment(file_name,original_data_size,train_percent,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path):
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    model = joblib.load(f'{"models"}/{train_percent}%_train/{model_name}_{file_name}.joblib')
    classify_process = Process(target=classify_module, args=(classify_queue, compress_queue,model))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path))
    classify_process.start()#start listening for data stream for classification
    compress_process.start()#start listening labeled chunks for compression and transfer
    #set_network_conditions("eth0", f'{network_speed}mbit', "0ms", "0%")#set the network speed

    print("loading data stream")
    for  i,chunk in enumerate(pd.read_csv(f'{"data/original"}/{file_name}.csv', chunksize=chunk_size, delimiter='|')):
        classify_queue.put(chunk)
    print("stream loaded")
    classify_queue.put(None)  # End of data stream signal
    print("classify start")
    classify_process.join() #wait the calssification module finish its jobs
    print("classify finish")
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")
    while transfer_queue.empty() != True:#load the outputs

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
    print("OUT: Compressiontime: ",total_compress_time)
    print("OUT: TransferTime: ",total_transfer_time)
    print("OUT: Classification time: ",classify_time)
    compressed_size = get_folder_size(os.path.join(compress_save_path, algorithm))
    print("OUT: Compressed size: ",compressed_size)
    compression_ratio =  original_data_size / compressed_size
    print("OUT: Compression ratio: ", compression_ratio)
    throughput= calculate_throughput(classification_time=classify_time,compression_time=total_compress_time,compression_ratio=compression_ratio,network_speed=5,data_size=original_data_size)
    print("OUT: Throughput: ",throughput)
    cost = calculate_cost(compression_ratio=compression_ratio,original_size=original_data_size,num_cores=24,compression_time=total_compress_time)
    print("OUT: Cost: ",cost)
    reset_network_conditions('eth0')
    return
def base_line(dataset,file_name,original_data_size,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,train_percent):
    result_folder = os.path.join('result', f'{train_percent}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    classify_process = Process(target=classify_module_base, args=(classify_queue, compress_queue))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed,'base'))
    classify_process.start()
    compress_process.start()#start listening labeled chunks for compression and transfer
    print('loading datastream')
    if dataset in ['flight']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter='|',quoting=3,quotechar='"')
    elif dataset in ['nypd','Submissions','100k_a','df_electronics']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter=',')
    else:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"')

    stream_start = time.time()
    for i, chunk in enumerate(iterator1):
        chunk  = chunk.sample(frac=1).reset_index(drop=True)
        classify_queue.put(chunk)
    stream_end = time.time()
    classify_queue.put(None)
    print("stream loaded: ",stream_end-stream_start)
    classify_process.join()
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")
    while transfer_queue.empty() != True:#load the outputs

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
            with open('results/transfer.txt', 'a') as tranfile:
                tranfile.write(f"transfer time: {total_transfer_time}")
    compression_times = {}
    with open('results/time.txt', 'r') as file:
        for line in file:
            # Split the line into worker ID and compression time
            parts = line.strip().split(': ')
            worker_id = parts[0].split('_')[1]  # Extract worker ID
            compression_time = float(parts[1])  # Convert compression time to float
            compression_times[worker_id] = compression_time
    open('results/time.txt', 'w').close()
    total_compress_time = max(compression_times.values())
    transfer_times = {}
    with open('results/trans_time.txt', 'r') as trans_file:
        for line in trans_file:
            # Split the line into worker ID and compression time
            parts = line.strip().split(': ')
            worker_id = parts[0].split('_')[1]  # Extract worker ID
            transfer_time = float(parts[1])  # Convert compression time to float
            transfer_times[worker_id] = transfer_time
    open('results/trans_time.txt', 'w').close()
    total_transfer_time = max(transfer_times.values())
    print("OUT: Compressiontime: ",total_compress_time)
    print("OUT: TransferTime: ",total_transfer_time)
    print("OUT: Classification time: ",classify_time)
    compressed_size = get_folder_size(os.path.join(compress_save_path, algorithm))
    print("OUT: Compressed size: ",compressed_size)
    compression_ratio =  original_data_size / compressed_size
    print("OUT: Compression ratio: ", compression_ratio)
    network_speed_fix = original_data_size/total_transfer_time
    throughput= calculate_throughput(classification_time=classify_time,compression_time=total_compress_time,compression_ratio=compression_ratio,network_speed=network_speed_fix,data_size=original_data_size)
    print("OUT: Throughput: ",throughput)
    total_time = total_compress_time+classify_time
    cost = calculate_cost(classify_time=classify_time,compression_times=compression_times,compression_ratio=compression_ratio,original_size=original_data_size,num_cores=11)
    print("OUT: Cost: ",cost)
    with open(f'{result_folder}/result_{file_name}_random_{algorithm}_{network_speed}.txt', "w") as result_file:
        result_file.write(f"Compressiontime: {total_compress_time:.4f}\n")
        result_file.write(f"TransferTime: {total_transfer_time:.4f}\n")
        result_file.write(f"Classification time: {classify_time:.4f}\n")
        result_file.write(f"Compressed size: {compressed_size:.7f}\n")
        result_file.write(f"Compression ratio: {compression_ratio:.4f}\n")
        result_file.write(f"Throughput: {throughput:.4f}\n")
        result_file.write(f"Cost: {cost:.4f}\n")
        result_file.write(f"NetworkSpeed: {network_speed:.4f}\n")
        result_file.write(f"Original_size: {original_data_size:.4f}\n")
        result_file.write(f"Model: {'Random'}\n")
    return
    return
def base_line_online(dataset,file_name,original_data_size,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,num_i,cat_i,delimiter):
    model_name = 'Online'
    result_folder = os.path.join('online', f'{dataset}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    classify_process = Process(target=classify_module_online, args=(classify_queue, compress_queue,num_i,cat_i))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed))
    compress_process.start()#start listening labeled chunks for compression and transfer
    classify_process.start()#start listening for data stream for classification
    print("loading data stream")
    iterator1 = pd.read_csv(f'data/original_data/{dataset}_10/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"',dtype=str)

    stream_start = time.time()
    for i, chunk in enumerate(iterator1):
        
        if dataset == 'orders':
            chunk[6] = chunk[6].astype(str).str.replace('Clerk#', '', regex=False)
            chunk[6] = chunk[6].astype(float)

        classify_queue.put(chunk)
    stream_end = time.time()
    classify_queue.put(None)
    print("stream loaded")
    print("classify start")
    classify_process.join() #wait the calssification module finish its jobs
    print("classify finish")
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")
    while transfer_queue.empty() != True:#load the outputs

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
    print("OUT: Compressiontime: ",total_compress_time)
    print("OUT: TransferTime: ",total_transfer_time)
    print("OUT: Classification time: ",classify_time)
    compressed_size = get_folder_size(os.path.join(compress_save_path, algorithm))
    print("OUT: Compressed size: ",compressed_size)
    compression_ratio =  original_data_size / compressed_size
    print("OUT: Compression ratio: ", compression_ratio)
    network_speed_fix = original_data_size/total_transfer_time
    throughput= calculate_throughput(classification_time=classify_time,compression_time=total_compress_time,compression_ratio=compression_ratio,network_speed=network_speed_fix,data_size=original_data_size)
    print("OUT: Throughput: ",throughput)
    cost = calculate_cost(compression_ratio=compression_ratio,original_size=original_data_size,num_cores=24,compression_time=total_compress_time)
    print("OUT: Cost: ",cost)
    with open(f'{result_folder}/result_{file_name}_{model_name}_{algorithm}_{network_speed}.txt', "w") as result_file:
        result_file.write(f"Compressiontime: {total_compress_time:.4f}\n")
        result_file.write(f"TransferTime: {total_transfer_time:.4f}\n")
        result_file.write(f"Classification time: {classify_time:.4f}\n")
        result_file.write(f"Compressed size: {compressed_size:.4f}\n")
        result_file.write(f"Compression ratio: {compression_ratio:.4f}\n")
        result_file.write(f"Throughput: {throughput:.4f}\n")
        result_file.write(f"Cost: {cost:.4f}\n")
        result_file.write(f"NetworkSpeed: {network_speed:.4f}\n")
        result_file.write(f"Original_size: {original_data_size:.4f}\n")
        result_file.write(f"Model: {model_name}\n")

    return
def expierment_km(dataset,file_name_og,file_name,original_data_size,train_percent,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,num_i,cat_i,delimiter,scaler_path):
    #data = read_data(file_name,num_i,cat_i,delimiter)
    result_folder = os.path.join('results', f'{train_percent}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    model = joblib.load(f'{"models"}/{dataset}/{model_name}_{overlap}_{train_percent}.joblib')
    #print('here')
    
    classify_process = Process(target=classify_module, args=(classify_queue, compress_queue,model,scaler_path))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed))
    classify_process.start()#start listening for data stream for classification
    compress_process.start()#start listening labeled chunks for compression and transfer
    #set_network_conditions("eth0", f'{network_speed}mbit', "0ms", "0%")#set the network speed
    print("loading data stream")
    '''
    if model_name in ['DecisionTree','GaussianNB']:
        stream_start = time.time()
        for  i,chunk in enumerate(pd.read_csv(f'{"data"}/{dataset}/{file_name_og}.csv', chunksize=chunk_size,header=None,delimiter='|')):
            print(i)
            classify_queue.put((chunk,chunk))
        stream_end = time.time()
        print("stream loaded: ",stream_end-stream_start)

    else:
    '''
    iterator1 = pd.read_csv(f'data/{dataset}/{file_name}.csv', chunksize=chunk_size, header=None)
    iterator2 = pd.read_csv(f'data/{dataset}/{file_name_og}.csv', chunksize=chunk_size, header=None, delimiter='|')

    stream_start = time.time()
    for i, (chunk1, chunk2) in enumerate(zip(iterator1, iterator2)):
        print(i)
        classify_queue.put((chunk1, chunk2))
    stream_end = time.time()
    print("stream loaded: ",stream_end-stream_start)
    classify_queue.put(None)  # End of data stream signal
    print("classify start")
    classify_process.join() #wait the calssification module finish its jobs
    print("classify finish")
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")
    while transfer_queue.empty() != True:#load the outputs

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
    print("OUT: Compressiontime: ",total_compress_time)
    print("OUT: TransferTime: ",total_transfer_time)
    print("OUT: Classification time: ",classify_time)
    compressed_size = get_folder_size(os.path.join(compress_save_path, algorithm))
    print("OUT: Compressed size: ",compressed_size)
    compression_ratio =  original_data_size / compressed_size
    print("OUT: Compression ratio: ", compression_ratio)
    network_speed_fix = original_data_size/total_transfer_time
    throughput= calculate_throughput(classification_time=classify_time,compression_time=total_compress_time,compression_ratio=compression_ratio,network_speed=network_speed_fix,data_size=original_data_size)
    print("OUT: Throughput: ",throughput)
    cost = calculate_cost(compression_ratio=compression_ratio,original_size=original_data_size,num_cores=24,compression_time=total_compress_time)
    print("OUT: Cost: ",cost)
    with open(f'{result_folder}/result_{file_name}_{model_name}_{algorithm}_{network_speed}.txt', "w") as result_file:
        result_file.write(f"Compressiontime: {total_compress_time:.4f}\n")
        result_file.write(f"TransferTime: {total_transfer_time:.4f}\n")
        result_file.write(f"Classification time: {classify_time:.4f}\n")
        result_file.write(f"Compressed size: {compressed_size:.4f}\n")
        result_file.write(f"Compression ratio: {compression_ratio:.4f}\n")
        result_file.write(f"Throughput: {throughput:.4f}\n")
        result_file.write(f"Cost: {cost:.4f}\n")
        result_file.write(f"NetworkSpeed: {network_speed:.4f}\n")
        result_file.write(f"Original_size: {original_data_size:.4f}\n")
        result_file.write(f"Model: {model_name}\n")

    return
def expierment_scaler_kp(dataset,file_name,original_data_size,train_percent,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,scaler_path,num_i,cat_i,delimiter):
    #data = read_data(file_name,num_i,cat_i,delimiter)
    result_folder = os.path.join('kp', f'{train_percent}%')#create folder
    if dataset == 'orders_string':
        file_name ='orders_insert_0.8_string'
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    if dataset == "orders_string":
        model = joblib.load(f'{"models-kp"}/orders_10/{model_name}_{overlap}_{train_percent}.joblib')
        
    else:
        model = joblib.load(f'{"models-kp"}/{dataset}_10/{model_name}_{overlap}_{train_percent}.joblib')

    #print('here')
    classify_process = Process(target=classify_module_scaler_kp, args=(classify_queue, compress_queue,model,scaler_path,file_name,num_i,cat_i,model_name))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed,dataset))
    classify_process.start()#start listening for data stream for classification
    compress_process.start()#start listening labeled chunks for compression and transfer
    #set_network_conditions("eth0", f'{network_speed}mbit', "0ms", "0%")#set the network speed
    print("loading data stream")
    '''
    if model_name in ['DecisionTree','GaussianNB']:
        stream_start = time.time()
        for  i,chunk in enumerate(pd.read_csv(f'{"data"}/{dataset}/{file_name_og}.csv', chunksize=chunk_size,header=None,delimiter='|')):
            print(i)
            classify_queue.put((chunk,chunk))
        stream_end = time.time()
        print("stream loaded: ",stream_end-stream_start)

    else:
    '''
    print(dataset,model_name,overlap,network_speed)
    if dataset in ['flight']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter='|',quoting=3,quotechar='"')
    elif dataset in ['nypd','Submissions','df_electronics','100k_a']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter=',')
    elif dataset == 'orders_string':
        iterator1 = pd.read_csv(f'data/original_data_kp/orders_10/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='|',skipinitialspace=True)

    else:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"')

    stream_start = time.time()
    #it = enumerate(iterator1)
    for i, chunk in enumerate(iterator1):
        classify_queue.put(chunk)
    stream_end = time.time()
    print("stream loaded: ",stream_end-stream_start)
    classify_queue.put(None)  # End of data stream signal
    print("classify start")
    classify_process.join() #wait the calssification module finish its jobs
    print("classify finish")
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")
    while transfer_queue.empty() != True:#load the outputs

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
    compression_times ={}
    with open('results/time.txt', 'r') as file:
        for line in file:
            # Split the line into worker ID and compression time
            parts = line.strip().split(': ')
            worker_id = parts[0].split('_')[1]  # Extract worker ID
            compression_time = float(parts[1])  # Convert compression time to float
            compression_times[worker_id] = compression_time
    open('results/time.txt', 'w').close()
    total_compress_time = max(compression_times.values())
    transfer_times = {}
    with open('results/trans_time.txt', 'r') as trans_file:
        for line in trans_file:
            # Split the line into worker ID and compression time
            parts = line.strip().split(': ')
            worker_id = parts[0].split('_')[1]  # Extract worker ID
            transfer_time = float(parts[1])  # Convert compression time to float
            transfer_times[worker_id] = transfer_time
    open('results/trans_time.txt', 'w').close()
    total_transfer_time = max(transfer_times.values())
    print("OUT: Compressiontime: ",total_compress_time)
    print("OUT: TransferTime: ",total_transfer_time)
    print("OUT: Classification time: ",classify_time)
    compressed_size = get_folder_size(os.path.join(compress_save_path, algorithm))
    print("OUT: Compressed size: ",compressed_size)
    compression_ratio =  original_data_size / compressed_size
    print("OUT: Compression ratio: ", compression_ratio)
    network_speed_fix = original_data_size/total_transfer_time
    throughput= calculate_throughput(classification_time=classify_time,compression_time=total_compress_time,compression_ratio=compression_ratio,network_speed=network_speed_fix,data_size=original_data_size)
    print("OUT: Throughput: ",throughput)
    cost = calculate_cost(classify_time=classify_time,compression_times=compression_times,compression_ratio=compression_ratio,original_size=original_data_size,num_cores=11)
    print("OUT: Cost: ",cost)
    with open(f'{result_folder}/result_{file_name}_{model_name}_{algorithm}_{network_speed}.txt', "w") as result_file:
        result_file.write(f"Compressiontime: {total_compress_time:.4f}\n")
        result_file.write(f"TransferTime: {total_transfer_time:.4f}\n")
        result_file.write(f"Classification time: {classify_time:.4f}\n")
        result_file.write(f"Compressed size: {compressed_size:.4f}\n")
        result_file.write(f"Compression ratio: {compression_ratio:.4f}\n")
        result_file.write(f"Throughput: {throughput:.4f}\n")
        result_file.write(f"Cost: {cost:.4f}\n")
        result_file.write(f"NetworkSpeed: {network_speed:.4f}\n")
        result_file.write(f"Original_size: {original_data_size:.4f}\n")
        result_file.write(f"Model: {model_name}\n")
    #del model
    #del iterator1
    #del classify_queue 
    #del compress_queue 
    #del transfer_queue
    #del it 
    gc.collect()
    return
def expierment_scaler_km(dataset,file_name,original_data_size,train_percent,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,scaler_path,num_i,cat_i,delimiter):
    #data = read_data(file_name,num_i,cat_i,delimiter)
    print(dataset,model_name,overlap,network_speed)
    result_folder = os.path.join('km', f'{train_percent}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    model = joblib.load(f'{"models-km"}/{overlap}/{model_name}_{dataset}.joblib')
    #print('here')
    classify_process = Process(target=classify_module_scaler_km, args=(classify_queue, compress_queue,model,scaler_path,model_name))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed,dataset))
    classify_process.start()#start listening for data stream for classification
    compress_process.start()#start listening labeled chunks for compression and transfer
    #set_network_conditions("eth0", f'{network_speed}mbit', "0ms", "0%")#set the network speed
    print("loading data stream")
    '''
    if model_name in ['DecisionTree','GaussianNB']:
        stream_start = time.time()
        for  i,chunk in enumerate(pd.read_csv(f'{"data"}/{dataset}/{file_name_og}.csv', chunksize=chunk_size,header=None,delimiter='|')):
            print(i)
            classify_queue.put((chunk,chunk))
        stream_end = time.time()
        print("stream loaded: ",stream_end-stream_start)

    else:
    '''


    iterator1 = pd.read_csv(f'data/original_data_km/{overlap}/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"')

    stream_start = time.time()
    for i, chunk in enumerate(iterator1):
        classify_queue.put(chunk)
    del iterator1
    gc.collect()
    stream_end = time.time()
    print("stream loaded: ",stream_end-stream_start)
    classify_queue.put(None)  # End of data stream signal
    print("classify start")
    classify_process.join() #wait the calssification module finish its jobs
    print("classify finish")
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")
    while transfer_queue.empty() != True:#load the outputs

        check = transfer_queue.get()
        if type(check) != None and len(check)==4:
            total_compress_time,total_transfer_time,classify_time,_ = check
    compression_times ={}
    with open('results/time.txt', 'r') as file:
        for line in file:
            # Split the line into worker ID and compression time
            parts = line.strip().split(': ')
            worker_id = parts[0].split('_')[1]  # Extract worker ID
            compression_time = float(parts[1])  # Convert compression time to float
            compression_times[worker_id] = compression_time
    open('results/time.txt', 'w').close()
    total_compress_time = max(compression_times.values())
    transfer_times = {}
    with open('results/trans_time.txt', 'r') as trans_file:
        for line in trans_file:
            # Split the line into worker ID and compression time
            parts = line.strip().split(': ')
            worker_id = parts[0].split('_')[1]  # Extract worker ID
            transfer_time = float(parts[1])  # Convert compression time to float
            transfer_times[worker_id] = transfer_time
    open('results/trans_time.txt', 'w').close()
    total_transfer_time = max(transfer_times.values())
    print("OUT: Compressiontime: ",total_compress_time)
    print("OUT: TransferTime: ",total_transfer_time)
    print("OUT: Classification time: ",classify_time)
    compressed_size = get_folder_size(os.path.join(compress_save_path, algorithm))
    print("OUT: Compressed size: ",compressed_size)
    compression_ratio =  original_data_size / compressed_size
    print("OUT: Compression ratio: ", compression_ratio)
    network_speed_fix = original_data_size/total_transfer_time #network speed/# of worker
    throughput= calculate_throughput(classification_time=classify_time,compression_time=total_compress_time,compression_ratio=compression_ratio,network_speed=network_speed_fix,data_size=original_data_size)
    print("OUT: Throughput: ",throughput)
    cost = calculate_cost(classify_time=classify_time,compression_times=compression_times,compression_ratio=compression_ratio,original_size=original_data_size,num_cores=11)
    print("OUT: Cost: ",cost)
    with open(f'{result_folder}/result_{file_name}_{model_name}_{algorithm}_{network_speed}.txt', "w") as result_file:
        result_file.write(f"Compressiontime: {total_compress_time:.4f}\n")
        result_file.write(f"TransferTime: {total_transfer_time:.4f}\n")
        result_file.write(f"Classification time: {classify_time:.4f}\n")
        result_file.write(f"Compressed size: {compressed_size:.4f}\n")
        result_file.write(f"Compression ratio: {compression_ratio:.4f}\n")
        result_file.write(f"Throughput: {throughput:.4f}\n")
        result_file.write(f"Cost: {cost:.4f}\n")
        result_file.write(f"NetworkSpeed: {network_speed:.4f}\n")
        result_file.write(f"Original_size: {original_data_size:.4f}\n")
        result_file.write(f"Model: {model_name}\n")

    return
def expierment_scaler_kp_one(dataset,file_name,original_data_size,train_percent,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,scaler_path,num_i,cat_i,delimiter):
    #data = read_data(file_name,num_i,cat_i,delimiter)
    result_folder = os.path.join('kp', f'{train_percent}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    model = joblib.load(f'{"models-kp"}/{dataset}_10/{model_name}_{overlap}_{train_percent}.joblib')
    #print('here')
    classify_process = Process(target=classify_module_scaler_kp, args=(classify_queue, compress_queue,model,scaler_path,file_name,num_i,cat_i,model_name))
    compress_process = Process(target=compress_module_sig, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed))
    classify_process.start()#start listening for data stream for classification
    compress_process.start()#start listening labeled chunks for compression and transfer
    #set_network_conditions("eth0", f'{network_speed}mbit', "0ms", "0%")#set the network speed
    print("loading data stream")
    print(dataset,model_name,overlap,network_speed)
    if dataset in ['flight']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter='|',quoting=3,quotechar='"')
    elif dataset in ['nypd','df_electronics']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter=',')
        print('################################################################')
    else:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"')
        
    stream_start = time.time()
    for i, chunk in enumerate(iterator1):
        classify_queue.put(chunk)
    stream_end = time.time()
    print("stream loaded: ",stream_end-stream_start)
    classify_queue.put(None)  # End of data stream signal
    print("classify start")
    classify_process.join() #wait the calssification module finish its jobs
    print("classify finish")
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")


    return
def base_line_one(dataset,file_name,original_data_size,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,train_percent):
    result_folder = os.path.join('baseline', f'{train_percent}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    classify_process = Process(target=classify_module_base, args=(classify_queue, compress_queue))
    compress_process = Process(target=compress_module_sig, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path,network_speed))
    classify_process.start()
    compress_process.start()#start listening labeled chunks for compression and transfer
    print('loading datastream')
    if dataset in ['flight']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter='|',quoting=3,quotechar='"')
    elif dataset in ['nypd']:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size,delimiter=',')
    else:
        iterator1 = pd.read_csv(f'data/original_data_kp/{dataset}_10/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"')

    stream_start = time.time()
    for i, chunk in enumerate(iterator1):
        #chunk  = chunk.sample(frac=1, random_state=42)
        classify_queue.put(chunk)
    stream_end = time.time()
    classify_queue.put(None)
    print("stream loaded: ",stream_end-stream_start)
    classify_process.join()
    print("compress start")
    compress_process.join() #wait the compression module finish its jobs
    print("compress done")


def debug(dataset,file_name,original_data_size,train_percent,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path,overlap,scaler_path,num_i,cat_i,delimiter):
    #data = read_data(file_name,num_i,cat_i,delimiter)
    result_folder = os.path.join('debug', f'{train_percent}%')#create folder
    result_folder = os.path.join(result_folder,f'{file_name}')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    chunk_list =[]
    compressed_list = []
    iterator1 = pd.read_csv(f'data/original_data_km/{overlap}/{file_name}.csv', chunksize=chunk_size, header=None,delimiter='|',quoting=3,quotechar='"')
    lists_for_labels = [pd.DataFrame() for _ in range(10)]

    stream_start = time.time()
    save_folder = os.path.join(compress_save_path,algorithm)#create folder
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    j=0
    for i, chunk in enumerate(iterator1):

        if model_name != 'online':
            model = joblib.load(f'{"models-km"}/{overlap}/{model_name}_{dataset}.joblib')
            if model_name in ['DecisionTree','GaussianNB']:
                labeled_chunk = classify_chunk_km(chunk,model)
                labeled_chunk=labeled_chunk[0]
            else:     
                labeled_chunk = classify_chunk_scaler_km(chunk,model,scaler_path)
                labeled_chunk=labeled_chunk[0]
            #print(labeled_chunk.head)
            chunk_list.append(labeled_chunk)
            for label in range(10):
                cluster_data = labeled_chunk.loc[labeled_chunk['label'] == label]
                lists_for_labels[label] = pd.concat([lists_for_labels[label], cluster_data])
        else:
            if j >9:
                j = 0
            labeled_chunk = classify_chunk_base(chunk,j)
            j+=1
            chunk_list.append(labeled_chunk)
            for label in range(10):
                cluster_data = labeled_chunk.loc[labeled_chunk['label'] == label]
                lists_for_labels[label] = pd.concat([lists_for_labels[label], cluster_data])
    print(len(lists_for_labels))
    print(lists_for_labels[0].head)

    for j in range(len(lists_for_labels)):
        print(lists_for_labels[j].shape)
        comp_dic =compress(lists_for_labels[j],algorithm)[0]
        for key in comp_dic:
            output_name = f'worker_{j}_{key}_compressed.{algorithm}'#get file name
            output_path = os.path.join(save_folder, output_name)#get save path
            compressed_data = comp_dic[key]#get the column data
            with open(output_path, 'ab') as f:
                f.write(compressed_data)#create a compressed file for that column
    compressed_size = get_folder_size(save_folder)
    print(compressed_size)
    compression_ratio =  original_data_size / compressed_size
    with open(f'{result_folder}/result_{file_name}_{model_name}_{algorithm}_{network_speed}.txt', "w") as result_file:
        result_file.write(f"Compressed size: {compressed_size:.4f}\n")
        result_file.write(f"Compression ratio: {compression_ratio:.4f}\n")
        result_file.write(f"Original_size: {original_data_size:.4f}\n")
        result_file.write(f"Model: {model_name}\n")

    return
def main():
    '''
    original_data_sizes=[156.11,124.06,139.55] #################################
    scaler_data_size = 124.84
    file_path = 'data/original'
    dataset = 'DS_001' #################################

    train_percents = [0.8,0.2,0.1] #############################
    model_path = 'models'
    #
    model_names = ['DecisionTree','GaussianNB','LogisticRegression','MLP','QDA']
    alg = 'gzip'
    overlaps = ['insert','no_overlap','update'] ##############################
    cat_i = [0,1,2,3]
    num_i = []
    delimiter = '|'
    chunk_size = 10000
    file_name = f'{dataset}_{overlaps[0]}_{train_percents[0]}'
    target_path = 'target/path'
    networkspeeds =[25,20,15,10,5,2]
    target_ip = ''
    target_user = ""
    compress_save_path = f'data/compressed_data/debug/{train_percents[0]}%_train/{model_names[0]}_{file_name}_{overlaps[0]}'#KM
    scaler_path = f'scalers/{overlaps[0]}/{dataset}_scaler_{overlaps[0]}_{train_percents[0]}.joblib'#KM
    #for i in range(len(model_names)):
        #print(model_names[i])
    
    debug(dataset,file_name,original_data_sizes[0],train_percents[0],model_names[0],chunk_size,alg,10,target_ip,target_user,networkspeeds[0],compress_save_path,target_path,overlaps[0],scaler_path,num_i,cat_i,delimiter)
    '''
    
    ################################FINISHED:ds_001,DS_002,flight,nypd,orders,
    #original_data_sizes=[156.11,628.27,50.59,79.76,163.99,113.47] #################################124.90,502.62,
    #original_data_sizes=[124.90,502.62,40.47,63.81,131.18,90.79]
    #original_data_sizes=[140.49,565.43,45.54,71.78,147.59,102.13]
    #original_data_sizes=[80.61,64.50,72.55]

    original_data_sizes=[50.12,40.09,45.11] #################################113.47,131.18,147.590.8,1050,[861.74,969.43[163.99,129.92,146.17]
    scaler_data_size = 71.78
    file_path = 'data/original'
    dataset = 'flight_adjusted' #################################

    train_percents = [0.8,0.2,0.1] #############################0.8,,0.2,0.1
    model_path = 'models'
    #,'LogisticRegression','MLP',,'LogisticRegression',,'QDA'
    model_names = ['DecisionTree','GaussianNB','LogisticRegression','MLP','QDA']#'DecisionTree','GaussianNB','LogisticRegression','MLP','QDA'
    alg = ['gzip','lz4','zstd']
    overlaps = ['insert','no_overlap','update'] ##############################,'insert','no_overlap','update'
    cat_i = [0, 1, 3, 6]#[0, 5]#[0, 1, 2, 3] #[0, 5]#[0, 5]#[0, 1, 3, 6]
    num_i = [2, 5]#[3, 6]#[3, 6]#[3, 6]#[2, 5]
    delimiter = '|'
    chunk_size = 10000

    target_path = '' #put target path here
    networkspeeds =[25,20,15,10,5]#,20,15,10,5,2,20,15
    target_ip = '' #put target ip here
    target_user = '' #put user name here
    '''
    for x in range(len(original_data_sizes)):
        original_data_size = original_data_sizes[x]
        train_percent = train_percents[x]
        overlap = overlaps[x]
        file_name = f'{dataset}_{overlap}_{train_percent}'
        file_name_og = f'{dataset}_{overlap}_{train_percent}_raw'
        for z in range(len(alg)):
            for i in range(len(model_names)):
                for j in range(len(networkspeeds)):
                    model_name = model_names[i]
                    networkspeed = networkspeeds[j]
                    #scaler_path =f'models-kp/{dataset}_10/{overlap}_{train_percent}_scaler.joblib'#KP
                    scaler_path = f'scalers/{overlap}/{dataset}_scaler_{overlap}_{train_percent}.joblib'#KM
                    #compress_save_path = f'data/compressed_data/kp/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KP
                    compress_save_path = f'data/compressed_data/km/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KM
                    #if model_name in ['LogisticRegression','MLP','QDA']:
                        #original_data_size = scaler_data_size 
                    #expierment_scaler_kp(dataset,file_name,original_data_size,train_percent=train_percent,model_name=model_name,chunk_size=chunk_size,algorithm=alg[z],worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,scaler_path=scaler_path,num_i=num_i,cat_i=cat_i,delimiter=delimiter)
                    expierment_scaler_km(dataset,file_name,original_data_size,train_percent=train_percent,model_name=model_name,chunk_size=chunk_size,algorithm=alg[z],worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,scaler_path=scaler_path,num_i=num_i,cat_i=cat_i,delimiter=delimiter)
        '''
    for x in range(len(original_data_sizes)):
        original_data_size = original_data_sizes[x]
        train_percent = train_percents[x]
        overlap = overlaps[x]
        file_name = f'{dataset}_{overlap}_{train_percent}'
        file_name_og = f'{dataset}_{overlap}_{train_percent}_raw'
        for z in range(len(alg)):
            for i in range(len(model_names)):
                for j in range(len(networkspeeds)):
                    model_name = model_names[i]
                    networkspeed = networkspeeds[j]
                    #scaler_path =f'models-kp/{dataset}_10/{overlap}_{train_percent}_scaler.joblib'#KP
                    scaler_path = f'scalers/{overlap}/{dataset}_scaler_{overlap}_{train_percent}.joblib'#KM
                    #compress_save_path = f'data/compressed_data/kp/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KP
                    compress_save_path = f'data/compressed_data/km/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KM
                    #if model_name in ['LogisticRegression','MLP','QDA']:
                        #original_data_size = scaler_data_size 
                    #expierment_scaler_kp(dataset,file_name,original_data_size,train_percent=train_percent,model_name=model_name,chunk_size=chunk_size,algorithm=alg[z],worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,scaler_path=scaler_path,num_i=num_i,cat_i=cat_i,delimiter=delimiter)
                    expierment_scaler_km(dataset,file_name,original_data_size,train_percent=train_percent,model_name=model_name,chunk_size=chunk_size,algorithm=alg[z],worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,scaler_path=scaler_path,num_i=num_i,cat_i=cat_i,delimiter=delimiter)

    ##################################

'''
    original_data_sizes=[628.27,502.62,565.43] #################################
    file_path = 'data/original'
    dataset = 'nypd' #################################

    train_percents = [0.8,0.2,0.1] #############################
    model_path = 'models'
    #
    model_names = ['online']
    alg = 'gzip'
    overlaps = ['insert','no_overlap','update'] ##############################
    num_i = [0,5]
    cat_i = [3,6]
    delimiter = '|'
    chunk_size = 10000

    target_path = ''
    networkspeeds =[25,20,15,10,5,2]
    target_ip = ''
    target_user = ''
    for x in range(len(original_data_sizes)):
        original_data_size = original_data_sizes[x]
        train_percent = train_percents[x]
        overlap = overlaps[x]
        file_name = f'{dataset}_{overlap}_{train_percent}'
        file_name_og = f'{dataset}_{overlap}_{train_percent}_raw'
        for i in range(len(model_names)):
            for j in range(len(networkspeeds)):
                model_name = model_names[i]
                networkspeed = networkspeeds[j]

                compress_save_path = f'data/compressed_data/online/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KP
                base_line_online(dataset,file_name,original_data_size,chunk_size=chunk_size,algorithm=alg,worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,num_i=num_i,cat_i=cat_i,delimiter=delimiter)
    return
'''

'''

#################################

#original_data_sizes=[156.11,628.27,50.59,79.76,163.99,113.47] #################################124.90,502.62,
#original_data_sizes=[40.47,63.81,131.18,90.79]
#original_data_sizes=[156.11]#140.49,565.43,45.54,71.78,147.59,102.13
original_data_sizes=[90.79]#,628.27,50.59,79.76,163.99,113.47
file_path = 'data/original'
datasets = ['partsupp'] #################################,'DS_002','flight','nypd','orders','partsupp'

train_percents = [0.2] #############################
model_path = 'models'
#
model_names = ['baseline']
algs = ['gzip']
overlaps = ['no_overlap'] ##############################
delimiter = '|'
chunk_size = 10000

target_path = ''
networkspeeds =[25,20,15,10,5]#,10,5,2
target_ip = ''
target_user = ''
for i in range(len(datasets)):
    for j in range(len(algs)):
        dataset = datasets[i]
        original_data_size = original_data_sizes[i]
        train_percent = train_percents[0]
        model_name = model_names[0]
        overlap = overlaps[0]
        file_name = f'{dataset}_{overlap}_{train_percent}'
        alg = algs[j]
        for k in range(len(networkspeeds)):
            network_speed = networkspeeds[k]
            compress_save_path = f'data/compressed_data/baseline/{model_name}_{file_name}_{overlap}'
            print(dataset,model_name,network_speed)
            base_line(dataset,file_name,original_data_size,chunk_size,alg,10,target_ip,target_user,network_speed,compress_save_path,target_path,overlap,train_percents[0])
'''
'''
################################# singal compress base

    #original_data_sizes=[156.11,628.27,50.59,79.76,163.99,113.47] #################################124.90,502.62,
    #original_data_sizes=[40.47,63.81,131.18,90.79]
    #original_data_sizes=[156.11]#140.49,565.43,45.54,71.78,147.59,102.13
original_data_sizes=[156.11]#,628.27,50.59,79.76,163.99,113.47
file_path = 'data/original'
datasets = ['DS_001'] #################################,'DS_002','flight','nypd','orders','partsupp'

train_percents = [0.8] #############################
model_path = 'models'
#
model_names = ['baseline']
alg = 'gzip'
overlaps = ['insert'] ##############################
delimiter = '|'
chunk_size = 10000

target_path = ''
networkspeeds =[25]#,20,15,10,5,2
target_ip = ''
target_user = ''
for i in range(len(datasets)):
    dataset = datasets[i]
    original_data_size = original_data_sizes[i]
    train_percent = train_percents[0]
    model_name = model_names[0]
    overlap = overlaps[0]
    file_name = f'{dataset}_{overlap}_{train_percent}'
    for j in range(len(networkspeeds)):
        network_speed = networkspeeds[j]
        compress_save_path = f'data/compressed_data/baseline/{model_name}_{file_name}_{overlap}'
        print(dataset,model_name,network_speed)
        base_line_one(dataset,file_name,original_data_size,chunk_size,alg,10,target_ip,target_user,network_speed,compress_save_path,target_path,overlap,train_percents[0])
'''

'''
################################ single compress kp
#original_data_sizes=[156.11,628.27,50.59,79.76,163.99,113.47] #################################124.90,502.62,
#original_data_sizes=[124.90,502.62,40.47,63.81,131.18,90.79]
#original_data_sizes=[140.49,565.43,45.54,71.78,147.59,102.13]

original_data_sizes=[156.11] #################################113.47,131.18,147.59
scaler_data_size = 124.84
file_path = 'data/original'
dataset = 'DS_001' #################################

train_percents = [0.8] #############################0.8,,0.2,0.1
model_path = 'models'
#,'LogisticRegression','MLP',,'LogisticRegression',,'QDA'
model_names = ['LogisticRegression']#'DecisionTree','GaussianNB','LogisticRegression','MLP','QDA'
alg = 'gzip'
overlaps = ['insert'] ##############################,'no_overlap','update'
cat_i = [0, 5]#[10, 11, 12, 13] #[0, 5]#[0, 1, 3, 6]
num_i = [3, 6]#[2, 3, 4, 5, 6, 7, 8, 16, 17, 18, 19]##[3, 6]#[2, 5]
delimiter = '|'
chunk_size = 10000

target_path = ''
networkspeeds =[25]#,20,15,10,5,2
target_ip = ''
target_user = ''
for x in range(len(original_data_sizes)):
    original_data_size = original_data_sizes[x]
    train_percent = train_percents[x]
    overlap = overlaps[x]
    file_name = f'{dataset}_{overlap}_{train_percent}'
    file_name_og = f'{dataset}_{overlap}_{train_percent}_raw'
    for i in range(len(model_names)):
        for j in range(len(networkspeeds)):
            model_name = model_names[i]
            networkspeed = networkspeeds[j]
            scaler_path =f'models-kp/{dataset}_10/{overlap}_{train_percent}_scaler.joblib'#KP
            #scaler_path = f'scalers/{overlap}/{dataset}_scaler_{overlap}_{train_percent}.joblib'#KM
            compress_save_path = f'data/compressed_data/kp/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KP
            #compress_save_path = f'data/compressed_data/km/{train_percent}%_train/{model_name}_{file_name}_{overlap}'#KM
            #if model_name in ['LogisticRegression','MLP','QDA']:
                #original_data_size = scaler_data_size 
            expierment_scaler_kp_one(dataset,file_name,original_data_size,train_percent=train_percent,model_name=model_name,chunk_size=chunk_size,algorithm=alg,worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,scaler_path=scaler_path,num_i=num_i,cat_i=cat_i,delimiter=delimiter)
            #expierment_scaler_km(dataset,file_name,original_data_size,train_percent=train_percent,model_name=model_name,chunk_size=chunk_size,algorithm=alg,worker_num=10,targe_tip=target_ip,target_user=target_user,network_speed=networkspeed,compress_save_path=compress_save_path,target_path=target_path,overlap=overlap,scaler_path=scaler_path,num_i=num_i,cat_i=cat_i,delimiter=delimiter)
'''
if __name__ == "__main__":
    main()