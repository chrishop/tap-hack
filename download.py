#!/usr/bin/env python3

import sys
import subprocess
import os

class Download:
    
    @staticmethod
    def go(table_name, id_name, the_min, the_max, batch_size):
        URL = "http://api.skymapper.nci.org.au/public/tap"
        # dr1.dr1p1_master
        filename = f"download_{table_name}_{the_min}-{the_max}.sh"
        with open(filename, 'w+') as f:
            for batch_min, batch_max in Download.generate_batch_queue(the_min, the_max, batch_size):
                f.write(
                    Download.generate_command(
                        URL,
                        Download.generate_range_query(table_name,
                                                            id_name,
                                                            batch_min,
                                                            batch_max
                        ),
                        Download.generate_filename(table_name, batch_min, batch_max)
                    )
                )
        return filename
        
    
    @staticmethod
    def generate_command(url, query, filepath):
        return(f"java -jar stilts.jar "
                f"tapquery tapurl='{url}' "
                f"adql='{query}' "
                f"out='{filepath}'\n")
    
    @staticmethod
    def generate_batch_queue(the_min, the_max, batch_size):
        queue = [[the_min, the_min]]
        batch_min = the_min
        batch_max = the_min

        iterations = int((the_max - the_min) / batch_size)
        for i in range(iterations):
            batch_min = batch_max
            batch_max = batch_max + batch_size
            queue.append([batch_min, batch_max])
        
        remainder = (the_max - the_min) % batch_size
        if remainder != 0:
            queue.append([batch_max, batch_max + remainder])
        return queue
    
    @staticmethod
    def generate_filename(table, batch_min, batch_max):
        return f"results/{table}_{batch_min}-{batch_max}.fits"

    @staticmethod
    def generate_range_query(table, id_name, batch_min, batch_max):
        return (f"SELECT * FROM {table} "
                f"WHERE {id_name}>{batch_min} AND {id_name}<={batch_max}")
    
    @staticmethod
    def generate_id_query(table, id_name, id_value):
        return f"SELECT * FROM {table} WHERE {id_name}={id_value}"
    
    @staticmethod
    def show_details(table_name, id_name, the_min, the_max, batch_size):
        return (f"downloading {table_name} by {id_name}\n"
                f"from {float(the_min)/(10**6)}m to {float(the_max)/10**6}m\n"
                f"in batches of {float(batch_size)/10**6}m\n")

if __name__ == "__main__":
    if len(sys.argv) == 6:
        if subprocess.run(['mkdir', 'results']).returncode == 0:
            print("created 'results/'")
        
        table_name = sys.argv[1]
        id_name = sys.argv[2]
        the_min = sys.argv[3]
        the_max = sys.argv[4]
        batch_size = sys.argv[5]
        
        print(
            Download.show_details(
                table_name,
                id_name,
                the_min,
                the_max,
                batch_size
            )
        )
        
        filename = Download.go(
            table_name,
            id_name,
            int(the_min),
            int(the_max),
            int(batch_size)
        )
        print(f"generated {filename}")
        print(f"running {filename}")
        subprocess.run(['chmod', '+x', os.path.abspath(filename)])
        subprocess.run(['/usr/bin/env', 'bash', os.path.abspath(filename)])
        print(f"finished {filename}") 
        print(f"deleting {filename}")
        subprocess.run(['rm', os.path.abspath(filename)])
    else:
        print("don't think you have the right amount of arguments")
        
   
