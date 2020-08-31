import os
import time
import ray
from contextlib import redirect_stdout, redirect_stderr

ray.init(address='auto')
log_dir = "/home/ubuntu/efs/logs"

@ray.remote
def write_file(task_idx):
    time.sleep(5)

    tasks_log_dir = log_dir + '/tasks'
    os.makedirs(tasks_log_dir, exist_ok=True)
    with open(tasks_log_dir +  f'/task_{task_idx}.log', 'a') as log_file:
        with redirect_stdout(log_file):
            with redirect_stderr(log_file):
                print(f"Logging from task {task_idx}!")
    return task_idx

def main():
    num_tasks = 40
    futures = [write_file.options(num_cpus=1).remote(task_idx) for task_idx in range(num_tasks)]
    results = ray.get(futures)
    print(results)

if __name__=="__main__":
    main()
