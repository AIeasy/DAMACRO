import subprocess
import time
def transfer_directory_via_ssh(local_dir_path, remote_username, remote_host, remote_dir_path):
    cmd = ["scp", "-r", local_dir_path, f"{remote_username}@{remote_host}:{remote_dir_path}"]
    subprocess.run(cmd)

def transfer_file_via_ssh(local_file_path, remote_username, remote_host, remote_file_path):
   '''
    cmd = ["scp", local_file_path, f"{remote_username}@{remote_host}:{remote_file_path}"]   
    start_time = time.time()
    subprocess.run(cmd)
    end_time = time.time()
    duration = end_time - start_time
    return duration
    '''
   return 5
def set_network_conditions(interface, rate, latency, loss):
  
    subprocess.call(["sudo", "tc", "qdisc", "del", "dev", interface, "root"])
    
 
    subprocess.call(["sudo", "tc", "qdisc", "add", "dev", interface, "root", "netem", 
                     "rate", rate, "delay", latency, "loss", loss])

def reset_network_conditions(interface):

    cmd_check = ["tc", "-s", "qdisc", "show", "dev", interface]
    result = subprocess.run(cmd_check, capture_output=True, text=True)

    if "netem" in result.stdout:  
        subprocess.call(["sudo", "tc", "qdisc", "del", "dev", interface, "root"])