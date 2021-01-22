from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
import os
import yaml

class RetriveAndWriteOpensearch:
    ''' Class for querying DHuS Opensearch API and write results to file
    '''

    def __init__(self, uname, pw, api_url, footprint, dates, query_kwargs,timeout=None):
        self.uname = uname
        self.pw = pw
        self.api_url = api_url
        self.footprint = footprint
        self.dates = dates
        self.query_kwargs = query_kwargs
        self.timeout = timeout

        self.api, self.products_df = self.main()
        self.products_count = self.products_df.shape[0]

    def main(self):
        """ Query api_url according to query_kwargs

        Args:
            self
        Returns:
            products_df: returns a pandas dataframe of the query ouput
        """

        if self.timeout:
            api = SentinelAPI(self.uname, self.pw, self.api_url,timeout=self.timeout)
        else:
            api = SentinelAPI(self.uname, self.pw, self.api_url)


        fp = geojson_to_wkt(read_geojson(self.footprint))

        products = api.query(area=fp, date=self.dates,
                             **self.query_kwargs)
        return api,api.to_dataframe(products)

    def write_title(self, filename, fpath=None):
        """ Write product title to file

        Args:
            filename: the output filename
            fpath: the output filepath (optional)
        Returns:
            [bool]: [return True if the output file is created and false if not]
        """
        product_title = list(self.products_df.title)
        if not fpath:
            output = filename
        else:
            output = '/'.join((fpath,filename))

            #create directory if not exists
            if not os.path.exists(fpath):
                os.makedirs(fpath)

        with open(output,'w') as outfile:
            for title in product_title:
                outfile.write(title)
                outfile.write('\n')

        return os.path.isfile(output)

    def verify_output(self):
        """ Verify that number of files written to file is equal to
        number of files available in the API

        Args:
            self
        Returns:
            [bool]: [return True if the number is equal and false if not]
        """
        fp = geojson_to_wkt(read_geojson(self.footprint))
        api_count = self.api.count(area=fp, date=self.dates,
                             **self.query_kwargs)
        print("Nb products retrieved: {}".format(self.products_count))
        print("Nb products available: {}".format(api_count))
        if api_count==self.products_count:
            return True
        else:
            return False


if __name__=='__main__':
    # read config data
    with open('../input_params.yaml') as input_params:
        try:
            input_p=yaml.safe_load(input_params)
        except yaml.YAMLError as exc:
            print(exc)


    general_input = input_p['general']
    endpoints = input_p['endpoints']
    platforms = input_p['platforms']

    base_output_paht = general_input['base_output_paht']

    timeout = 300 #s


    # Create the various queries

    api_url = input_p['endpoints']['colhub']['api_url']
    uname = input_p['endpoints']['colhub']['uname']
    pw = input_p['endpoints']['colhub']['pw']

    date_key, date_value = "201803",("20180301","20180304")
    footprint = '../'+input_p['platforms']['sentinel-1']['aois']['mainland']
    kwarg = input_p['platforms']['sentinel-1']['kwargs']['SLC_iw']

    fpath = '/'.join((os.getcwd(),"output"))
    print(fpath)
    fname = "{}_test.txt".format(api_url.split('/')[2])

    test = RetriveAndWriteOpensearch(uname,
                                     pw,
                                     api_url,
                                     footprint,
                                     date_value,
                                     kwarg,
                                     timeout=timeout
                                     )
    if test.products_count>0:
        print("Number of products: {}".format(test.products_count))
        writing_works = test.write_title(fname,fpath)
        print("Writing works: ",writing_works)
    test_counting = test.verify_output()
    if test_counting:
        print("Nb products verification works")
