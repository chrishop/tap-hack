#!/usr/bin/env python3

import sys, subprocess

def delete(table_name, the_min, the_max, batch_size):
    queue = generate_batch_queue(int(the_min), int(the_max), int(batch_size))
    for batch_min, batch_max in queue:
        filename = generate_filename(table_name, batch_min, batch_max)
        remove("results/" + filename)
        print(f"deleted {filename}")
    
def generate_filename(table, batch_min, batch_max):
        return f"{table}_{batch_min}-{batch_max}.fits"
    
def generate_batch_queue(the_min, the_max, batch_size):
        queue = []
        batch_min = the_min
        batch_max = the_min

        iterations = int((the_max - the_min) / batch_size)
        for _ in range(iterations):
            batch_min = batch_max
            batch_max = batch_max + batch_size
            queue.append([batch_min, batch_max])
        
        remainder = (the_max - the_min) % batch_size
        if remainder != 0:
            queue.append([batch_max, batch_max + remainder])
        return queue
    
def remove(filename):
    subprocess.run(['rm', filename])

if __name__ == "__main__":
    if len(sys.argv) == 5:
        table_name = sys.argv[1]
        the_min = sys.argv[2]
        the_max = sys.argv[3]
        batch_size = sys.argv[4]
        
        delete(table_name, the_min, the_max, batch_size)
    else:
        print("Incorrect arguments number")
        