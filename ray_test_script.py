import os
import time
import ray
from contextlib import redirect_stdout, redirect_stderr

info = ray.init(address='auto')
log_dir = "/home/ubuntu/efs/logs"
duration = 3         # "compute time"
num_nodes_req = 3.0  # (head + num workers) specified in the aws_example.yaml file

@ray.remote
def write_file(task_idx):
    time.sleep(duration)  # "compute time"
    tasks_log_dir = log_dir + '/tasks'
    os.makedirs(tasks_log_dir, exist_ok=True)
    with open(tasks_log_dir +  f'/task_{task_idx}.log', 'w') as log_file:
        with redirect_stdout(log_file):
            with redirect_stderr(log_file):
                print(f"Logging from task {task_idx}, node ip: {info['node_ip_address']}!")
                print(info)
                print()
    return task_idx

def print_node_summary(nodes):
    for node in nodes:
        for key, val in node.items():
            print(f"{key}: {val}")
        print()

def main():
    print("Current cluster resources: ")
    print(ray.available_resources())

    # Waiting for nodes to start up
    wait_time = 0
    num_nodes_avail = ray.available_resources()['CPU']  # 1 CPU per node on t2.micro
    while num_nodes_avail < num_nodes_req:
        if wait_time > 20:
            print(f"Timing out... please run 'ray monitor aws_example' to see if nodes were created properly.")
            exit()
        else:
            print(f"Currently only have {num_nodes_avail}/{num_nodes_req} requested node(s)/CPU(s) available. Waiting...")
            print(ray.available_resources())
            time.sleep(3)
            wait_time += 3
            num_nodes_avail = ray.available_resources()['CPU']  # 1 CPU per node on t2.micro

    # All requested nodes up, ready to start    
    print(f"\n All ({num_nodes_avail}/{num_nodes_req}) nodes available! Printing node information:")
    print_node_summary(ray.nodes())
    
    # Launching tasks
    num_tasks = 20
    start = time.time()
    refs = [write_file.options(num_cpus=1).remote(task_idx) for task_idx in range(num_tasks)]
    waiting_refs = list(refs)
    while len(waiting_refs) > 0:
        return_n = 2 if len(waiting_refs) > 1 else 1
        ready_refs, remaining_refs = ray.wait(waiting_refs, num_returns=return_n, timeout=10.0)
        results = ray.get(ready_refs)
        for task_idx in results:
            print(f"Task {task_idx} has completed on node {info['node_ip_address']}.")  # Node ip address not working properly atm??
        waiting_refs = remaining_refs
    stop = time.time()

    print(f"\nTook {stop - start} seconds to complete with {num_nodes_avail} nodes, would take approx. {num_tasks * duration} seconds sequentially.")

if __name__=="__main__":
    main()
