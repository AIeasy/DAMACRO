from multiprocessing import Process, Queue, Event
import time
from classify import classify_chunk
import joblib
import pandas as pd
import os
from transfer import transfer_file_via_ssh,set_network_conditions,reset_network_conditions
import lz4.frame
import gzip
import zstandard as zstd
import subprocess
def calculate_cost(
        compression_time: float,
        compression_ratio: float,
        original_size: float,
        num_cores: int,
        cost_scale = 'TB', # calculate the cost of handreds of TBs
        p_cpu = 0.048,
        p_net = 0.05,
):

    base_cost = ((p_cpu / 3600) * num_cores) * compression_time + ((original_size) * p_net / compression_ratio)
    cost = base_cost * 1024 * 1024
    return cost
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
def compress(data, compression_algorithm):
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
        #column_data = "  "
        if compression_algorithm == 'lz4':
            compressed_data = lz4.frame.compress(column_data.to_csv(index=False).encode())
            compressed_columns[column] = compressed_data
        elif compression_algorithm == 'gzip':
            compressed_data = gzip.compress(column_data.to_csv(index=False).encode())
            compressed_columns[i] = compressed_data
        elif compression_algorithm == 'zstd':
            cctx = zstd.ZstdCompressor(level=3)
            compressed_data = cctx.compress(column_data.to_csv(index=False).encode())
            compressed_columns[column] = compressed_data
        else:
            raise ValueError(f"Unsupported compression algorithm: {compression_algorithm}")
        i+=1
    end_time = time.time()
    compress_time = end_time - start_time

    return compressed_columns, compress_time
def transfer_process(worker_id, transfer_input_queue,output_queue,compression_algorithm,s_path, remote_username, remote_host, remote_file_path):
    print(f'transfer_{worker_id}_start\n')
    save_folder = os.path.join(s_path, compression_algorithm)#create folder
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    process_transfer_time =0
    while True:
        compress_tuple = transfer_input_queue.get()
        if len(compress_tuple) == 4:  # Shutdown signal,now the compression is done, we can start transfer
            print(f'transfer_{worker_id}_recived\n')
            _,worker_id, total_compress_time,compressed_dic = compress_tuple
            if len(compressed_dic)!=0:
                for key in compressed_dic:
                    output_name = f'worker_{worker_id}_{key}_compressed.{compression_algorithm}'#get file name
                    output_path = os.path.join(save_folder, output_name)#get save path
                    compressed_data = compressed_dic[key]#get the column data
                    with open(output_path, 'ab') as f:
                        f.write(compressed_data)#create a compressed file for that column

                start_transfer_time = time.time()
                for filename in os.listdir(save_folder):#loop throgh all columns files saved in folder
                    file_path = os.path.join(save_folder, filename)
                    if (f'worker_{worker_id}' in filename):
                        #print(f'transfer_{worker_id} is sending last pice\n')
                        #transfer_file_via_ssh(local_file_path=output_path, remote_username=remote_username, remote_host=remote_host, remote_file_path=remote_file_path)#transfer it
                        cmd = ["scp", file_path, f"{remote_username}@{remote_host}:{remote_file_path}"]
                        subprocess.run(cmd)
                        #print(f'transfer_{worker_id} done sending last pice\n')
                end_transfer_time = time.time()
                process_transfer_time += end_transfer_time - start_transfer_time
                output_queue.put((None,total_compress_time,process_transfer_time))#transfer is finished
                print(f'transfer_{worker_id}_stopped_l\n')
                break
            else:
                print(f'transfer_{worker_id}_stopped\n')
                break
        else:
            #update the compressed file with new chunks
            worker_id, total_compress_time,compressed_dic = compress_tuple
            for key in compressed_dic:
                output_name = f'worker_{worker_id}_{key}_compressed.{compression_algorithm}'#get file name
                output_path = os.path.join(save_folder, output_name)#get save path
                output_gay = os.path.join(save_folder, f'worker_{worker_id}_{key}_done.{compression_algorithm}')
                compressed_data = compressed_dic[key]#get the column data
                with open(output_path, 'ab') as f:
                    f.write(compressed_data)#create a compressed file for that column
            '''
            with open(output_gay,'w') as f: # testing the order of file generation
                f.write("gay")
            
            start_transfer_time = time.time() #if you want pass the file everytime it updates
            for filename in os.listdir(save_folder):#loop throgh all columns files saved in folder
                file_path = os.path.join(output_path, filename)
                if (f'worker_{worker_id}' in filename):
                    print(f'transfer_{worker_id} is sending\n')
                    transfer_file_via_ssh(local_file_path=1, remote_username=1, remote_host=1, remote_file_path=1)#transfer it
                    print(f'transfer_{worker_id} done sending\n')
            end_transfer_time = time.time()
            process_transfer_time += end_transfer_time - start_transfer_time

            output_queue.put((total_compress_time,process_transfer_time))#feed back the output
            '''
def worker_process(worker_id, input_queue, output_queue,alg,path):
    print(f'worker_{worker_id}_start\n')
    total_compress_time = 0
    accumulated_data = pd.DataFrame()
    while True:
        data = input_queue.get()
        if data is None:  # Shutdown signal
            if not accumulated_data.empty:
                compressed_data,compress_time = compress(accumulated_data,alg)#get leftover compressed_dic
                #transfer_time = transfer(save_folder)
                total_compress_time += compress_time
                #total_transfer_time += transfer_time
                output_queue.put((None,worker_id, total_compress_time,compressed_data))#feed the leftover compressed_dic into transfer and tell transfer to stop
                print(f'worker_{worker_id}_end_l\n')
                break
            output_queue.put((None,worker_id, total_compress_time,""))#feed the leftover compressed_dic into transfer and tell transfer to stop
            print(f'worker_{worker_id}_end\n')
            break
        
        accumulated_data = pd.concat([accumulated_data, data])
        if len(accumulated_data) >= 10000:

            compressed_data,compress_time = compress(accumulated_data,alg)#get compressed_dic
           # transfer_time = transfer(save_folder)
            total_compress_time += compress_time
           # total_transfer_time += transfer_time
            output_queue.put((worker_id, total_compress_time,compressed_data))#feed the compressed_dic into transfer
            accumulated_data = pd.DataFrame()
def classify_module(input_queue, output_queue,model):
    start_time = time.time()
    while True:
        chunk = input_queue.get()
        if chunk is None:
            end_time = time.time()
            print(f"Classification Time: {end_time-start_time}")
            output_queue.put((None,end_time-start_time))
            break
        labeled_data = classify_chunk(chunk,model)

        output_queue.put(labeled_data)
    print("classify module is done")
    return
def compress_module(input_queue, output_queue,alg,path,num_worker, remote_username, remote_host, remote_file_path):
    print("in the compress module")

    worker_queues = {label: Queue() for label in range(num_worker)}  # for number of worker, create worker queues
    transfer_queues = {label: Queue() for label in range(num_worker)}#for number of worker, create transfer queues
    workers = [Process(target=worker_process, args=(label, worker_queues[label], transfer_queues[label],alg,path)) for label in range(num_worker)]#create worker process
    transfers = [Process(target=transfer_process, args=(label, transfer_queues[label],output_queue,alg,path, remote_username, remote_host, remote_file_path)) for label in range(num_worker)] #create transfer process

    for worker in workers:#starting worker listening for labeled chunk
        worker.start()
    for transfer in transfers:#start transfer listening for compressed_dic which cotains columns
        transfer.start()
    while True:
        labeled_chunk = input_queue.get()#get the lableed chunk

        if len(labeled_chunk) == 2:  # Shutdown signal
            _,classify_time = labeled_chunk
            print("time to stop")
            for q in worker_queues.values():        
                q.put(None)#stop worker
            break

        # Split the labeled_chunk DataFrame into clusters based on labels
        for label in range(num_worker):
            cluster_data =labeled_chunk.loc[labeled_chunk['label'] == label]
            worker_queues[label].put(cluster_data)
    '''
    while True:#listening for output 
        output = output_queue.get()
        if len(output)== 3:
            _,compress_time, transfer_time = output#get output from transferer proce
            total_compress_time += compress_time
            total_transfer_time += transfer_time
            break
        compress_time, transfer_time = output#get output from transferer process
        total_compress_time += compress_time
        total_transfer_time += transfer_time
        '''
    total_transfer_time =0
    start_time_c = time.time()#timing for compression time
    for worker in workers:#wait worker finish it jobs
        worker.join()
    end_time_c = time.time()
    print("Worker is done:D")
    start_time_t = time.time()#timing for transfer time
    for transfer in transfers:#wait transfer finish it jobs
        transfer.join()
    end_time_t = time.time()
    print("Trasfer is done :D")

    compress_time = end_time_c-start_time_c
    total_transfer_time = end_time_t-start_time_t
    print(f"Total Compression Time: {compress_time}")
    print(f"Total Tramsfer Time: {total_transfer_time}")
    output_queue.put((compress_time, total_transfer_time,classify_time,None))
    print("out compress model")
    return
def expierment(file_path,file_name,original_data_size,train_percent,model_path,model_name,chunk_size,algorithm,worker_num,targe_tip,target_user,network_speed,compress_save_path,target_path):
    classify_queue = Queue()
    compress_queue = Queue()
    transfer_queue = Queue()
    model = joblib.load(f'{model_path}/{train_percent}%_train/{model_name}_{file_name}.joblib')
    classify_process = Process(target=classify_module, args=(classify_queue, compress_queue,model))
    compress_process = Process(target=compress_module, args=(compress_queue, transfer_queue,algorithm,compress_save_path,worker_num,target_user,targe_tip,target_path))
    classify_process.start()#start listening for data stream for classification
    compress_process.start()#start listening labeled chunks for compression and transfer
    set_network_conditions("ens33", f'{network_speed}mbit', "0ms", "0%")#set the network speed
    print("loading data stream")
    for  i,chunk in enumerate(pd.read_csv(f'{file_path}/{file_name}.csv', chunksize=chunk_size, delimiter='|')):
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

def main():
    original_data_size=151.3
    file_path = 'data/original'
    file_name = 'econbiz'
    train_percent = 20
    model_path = 'models'
    model_name = 'AdaBoost'
    alg = 'gzip'
    networkspeed = 100
    chunk_size = 10000
    compress_save_path = f'data/compressed_data/{train_percent}%_train/{model_name}_{file_name}'
    target_path = '/home/sean/Desktop/'
    expierment(file_path,file_name,original_data_size,train_percent=20,model_path,model_name=model_name,chunk_size=chunk_size,algorithm=alg,worker_num=10,targe_tip='192.168.204.128',target_user='sean',network_speed=100,compress_save_path=compress_save_path,target_path=target_path)
    return

if __name__ == "__main__":
    main()
