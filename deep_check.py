#!/usr/bin/env python3
from astropy.table import Table
from astropy.io import fits
import numpy as np
import sys

def deep_check(fits_file):
    table = fits.open(fits_file, format='fits', memmap=True, mode='readonly')[1].data
    
    
    the_min = np.min(table["object_id"])
    the_max = np.max(table["object_id"])
    del table
    
    return [the_min, the_max]
        
        

def show(min_max):
    print(
        f"min object_id: {min_max[0]}\n"
        f"max object_id: {min_max[1]}"
    )

if __name__ == "__main__":
    if len(sys.argv) == 2:
        filename = sys.argv[1]
        
        show(deep_check(filename))
    else:
        print("Incorrect number of arguments")