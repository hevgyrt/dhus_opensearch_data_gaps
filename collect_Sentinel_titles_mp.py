"""
Script for harvesting content from various DHUS endpoints for Copernicus
Sentinel data, storing the data in text files.
"""

import yaml
import datetime
import numpy as np
import multiprocessing as mp
import os
import logging
from query_opensearch import RetriveAndWriteOpensearch
logging.basicConfig(filename='logfile.log', level=logging.DEBUG)

def retrieve_and_write(uname, pw, api_url,footprint,date_value,kwarg,timeout,fname,fpath):
    """ Calls the RetriveAndWriteOpensearch method and write output, if any, to
    text file.
    """
    #print('parent process:', os.getppid())
    #print('process id:', os.getpid())
    test = RetriveAndWriteOpensearch(uname,
                                     pw,
                                     api_url,
                                     footprint,
                                     date_value,
                                     kwarg,
                                     timeout=timeout
                                     )
    if test.products_count>0:
        test.write_title(fname,fpath)

def main():

    pool = mp.Pool(processes=4)

    # read config data
    with open('input_params.yaml') as input_params:
        try:
            input_p=yaml.safe_load(input_params)
        except yaml.YAMLError as exc:
            print(exc)


    general_input = input_p['general']
    endpoints = input_p['endpoints']
    platforms = input_p['platforms']

    base_output_paht = general_input['base_output_paht']

    # Make date intervals for the queries
    years = np.arange(2019,2021)
    months = np.arange(1,13)

    dates = {}
    for y in years:
        for i,m in enumerate(months):
            start_date = datetime.date(y,m,1)
            if m==12:
                end_date = datetime.date(y,1,1)
            else:
                end_date = datetime.date(y,m+1,1)

            date_key = start_date.strftime("%Y%m")

            dates[date_key]=(start_date.strftime("%Y%m%d"),end_date.strftime("%Y%m%d"))


    timeout = 300 #s


    # Create the various queries from the input parameters
    for k,v in endpoints.items():
        api_url = v['api_url']
        uname = v['uname']
        pw = v['pw']

        for platform, vv in platforms.items():
            for aoi, footprint in vv['aois'].items():
                for product_level, kwarg in vv['kwargs'].items():
                    for date_key, date_value in dates.items():
                        fpath = '/'.join((base_output_paht,platform,aoi,product_level,date_key))
                        fname = "{}.txt".format(api_url.split('/')[2])

                        pool.apply_async(retrieve_and_write, args=(uname, pw, api_url,footprint,date_value,kwarg,timeout,fname,fpath,))
                        break
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
