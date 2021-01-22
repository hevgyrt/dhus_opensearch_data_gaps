"""
Script for comparing the content in various DHUS endpoints for Copernicus
Sentinel data by means of producing difference files.
"""

import yaml
import numpy as np
import multiprocessing as mp
import os
import logging
import subprocess as sp
logging.basicConfig(filename='log_sort_diff.log', level=logging.DEBUG)

def sort_and_differentiate_files(file1, file2,folder):
    """Method for sorting, differentiating, and writing the difference (if any)
    to a text file

        sorting: uses the bash "sort" cmd
        comparing: uses the bash "comm" cmd showing the lines unique in file2
    """

    files_dict = {file1:"sorted_{}".format(file1),
                file2:"sorted_{}".format(file2)}

    # SORTING
    for k,v in files_dict.items():
        sort_cmd = ["sort", "-n", k, "-o", v]
        sort_pipe = sp.Popen(sort_cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE,cwd=folder)
        stdout,stderr = sort_pipe.communicate()
        #print(stdout, stderr)

    # COMPARING
    comm_cmd = ["comm", "-13", files_dict[file1], files_dict[file2]]#, ">diff.txt"]
    comm_pipe = sp.Popen(comm_cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE,cwd=folder)
    stdout,stderr = comm_pipe.communicate()

    # WRITE OUTPUT IF ANY
    if stdout:
        print(folder)
        with open ('{}/diff.txt'.format(folder),'w') as outfile:
            diff_content = (stdout).decode("utf-8") # from bytes to string
            outfile.write(diff_content)

def main():

    pool = mp.Pool(processes=4)

    # read config data
    with open('input_params.yaml') as input_params:
        try:
            input_p=yaml.safe_load(input_params)
        except yaml.YAMLError as exc:
            print(exc)


    general_input = input_p['general']

    base_output_paht = general_input['base_output_paht']

    # decide file1 and file2. Unique lines in file2 will be the output
    f1 = 'colhub.met.no.txt'
    f2 = 'scihub.copernicus.eu.txt'

    # Iterate through all folders with content
    for subdir, dirs, files in os.walk(base_output_paht):
        if f1 and f2 in files:
            pool.apply_async(sort_and_differentiate_files, args=(f1,f2,subdir,))

    pool.close()
    pool.join()

if __name__ == "__main__":
    main()
