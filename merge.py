#!/usr/bin/env python3

import sys, subprocess, os

def merge(table_name, the_min, the_max, batch_size, out_filename):
    merge_queue = generate_batch_queue(int(the_min), int(the_max), int(batch_size))
    
    first_elem = merge_queue.pop(0)
    rename(
        generate_filename(table_name, first_elem[0], first_elem[1]),
        "results/" + out_filename
    )
    print(show_details(table_name, first_elem[0], first_elem[1], out_filename))
    
    
    while len(merge_queue) > 0:
        elem = merge_queue.pop(0)
        batch_min = elem[0]
        batch_max = elem[1]
        
        merge_file = generate_filename(table_name, batch_min, batch_max)
        run_command(merge_file, "results/" + out_filename, "results/" + out_filename)
        print(show_details(table_name, batch_min, batch_max, out_filename))
        
        

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
    
def generate_filename(table, batch_min, batch_max):
        return f"results/{table}_{batch_min}-{batch_max}.fits"
    
def run_command(merge_file1, merge_file2, out_file):
    subprocess.run(["java", "-jar",
                    "stilts.jar", "tcat",
                    f"in={merge_file1}", f"in={merge_file2}", f"out={out_file}"])
    
def rename(old_name, new_name):
    subprocess.run(["mv", old_name, new_name])
    
def show_details(table_name, the_min, the_max, out_file):
        return (f"Merging {table_name} "
                f"Range {float(the_min)/(10**6)}m - {float(the_max)/10**6}m "
                f"into {out_file} ")
    
if __name__ == "__main__":
    if len(sys.argv) == 6:
        table_name = sys.argv[1]
        the_min = sys.argv[2]
        the_max = sys.argv[3]
        batch_size = sys.argv[4]
        out_file = sys.argv[5]
        
        merge(table_name, the_min, the_max,batch_size, out_file)
    else:
        print("Are there enough arguments?")