#!/usr/bin/env python3

import sys, os
import subprocess

def check(table, the_min, the_max, batch_size):
    not_present = []
    query_queue = generate_batch_queue(
        int(the_min), int(the_max), int(batch_size)
    )
    files = get_files('results')
    for batch_min, batch_max in query_queue:
        filename = generate_filename(table, batch_min, batch_max)
        if filename not in files:
            not_present.append(filename)
    return not_present

def show(not_present):
    print(not_present)
    if not_present == []:
        print("Great :) no files missing!")
    else:
        print("These are the files that are missing:")
        print("\n".join(not_present))
            
        
def generate_filename(table, batch_min, batch_max):
        return f"{table}_{batch_min}-{batch_max}.fits"

def generate_batch_queue(the_min, the_max, batch_size):
    queue = [[the_min, the_min]]
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
    
def get_files(folder):
    proc = subprocess.run(
        ['ls', os.path.abspath(folder)],
        stdout=subprocess.PIPE,
        universal_newlines=True
    )
    return proc.stdout.strip().split('\n')

if __name__ == "__main__":
    if len(sys.argv) == 5:
        table = sys.argv[1]
        the_min = sys.argv[2]
        the_max = sys.argv[3]
        batch_size = sys.argv[4]

        show(
            check(
                table,
                the_min,
                the_max,
                batch_size
            )
        )
    else:
        print("I think you are missing arguments")
        
    
    