#!/usr/bin/env python3

import datetime
import requests
import json
import sys
import logging
import time
import boto3
import os
import io
from timer import Timer
from subprocess import Popen, PIPE
from os import listdir
from os.path import isfile, join


NUM_OF_PRD = 2000
CLOUD_COVERAGE = 20
ALL_TIME_AVG_SIZE = 629145600
MAX_PROD_SIZE = 1029145600
MIN_PROD_SIZE = 729145600

START_DATE = (datetime.datetime.now() + datetime.timedelta(days=-30)).isoformat()

HOME = os.environ['HOME']
WORK_DIR = HOME + '/bench-front'
INDEX = WORK_DIR + '/item.md'
TEMPLATE = WORK_DIR + '/template.md'
TEMPLATE_DEFAULT = WORK_DIR + '/template_default.md'
RESULTS = '/results.out'

WWW_ROOT='/var/www/html'

S3_ENDPOINT = 'https://eocloud.eu:8080'
BUCKET = 'front-office-sample'

FINDER_API_URL = 'https://finder.eocloud.eu/resto/api/collections/Sentinel2/search.json?\
            maxRecords={0}&\
            processingLevel=LEVELL1C&\
            publishedAfter={1}&\
            cloudCover=[0,{2}]&\
            sortParam=startDate&sortOrder=descending&\
            dataset=ESA-DATASET'.format(NUM_OF_PRD, START_DATE, CLOUD_COVERAGE)


def find_products(url: str):

    logging.info('Finder URL: ' + url.replace(' ', ''))

    # pp(url.replace(' ', ''))
    r = requests.get(url)

    logging.info('Found ' + str(len(r.json()['features'])) + ' products')

    return r.json()


def select_product(products: json):
    from random import randint
    if not products:
        sys.exit(1)

    prods_narrow_selection = [p for p in products['features']
                              if (MAX_PROD_SIZE >= p['properties']['services']['download']['size'] >= MIN_PROD_SIZE)]

    logging.info('Narrow selection: ' + str(len(prods_narrow_selection)))

    r = randint(0, len(prods_narrow_selection) - 1)
    _product = prods_narrow_selection[r]

    logging.info('Selected product: ' + _product['properties']['title'])

    return _product


def convert_product(_product: json):
    if not _product:
        sys.exit(1)
    try:
        os.mkdir('{0}/{1}/'.format(WORK_DIR, _product['properties']['title']))
    except Exception as e:
        logging.error('Failed on mkdir {0}/{1}/. With error: {2}'
                      .format(WORK_DIR, _product['properties']['title'], str(e)))
        sys.exit(1)

    comm = '/usr/local/snap/bin/pconvert -f png -W 800 -p {0}/rgb_def.txt -o {0}/{1}/ {2}'\
        .format(WORK_DIR, _product['properties']['title'], _product['properties']['productIdentifier'])
    p = Popen(comm.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False, universal_newlines=True)
    (output, err) = p.communicate()
    logging.info(output)
    logging.info(err)
    return output.splitlines()[-1].split()[-1][1:-4]


def line_tune(line, best_product, output_file_png, duration, maxnum, now):
    if '_NOW' in line:
        line = line.replace('_NOW', now)
    if '_TAGS' in line:
        line = line.replace('_TAGS',', '.join([x['name'].encode('utf-8') for x in best_product['properties']['keywords'] if x['name'].encode('utf-8')[0] != '_'][:-6]))
    if '_MAXNUM' in line:
        line = line.replace('_MAXNUM', maxnum)
    if '_PRODNAME' in line:
        line = line.replace('_PRODNAME', best_product['properties']['title'])
    if '_PRODURL' in line:
        line = line.replace('_PRODURL', S3_ENDPOINT+"/swift/v1/"+BUCKET+"/png/"+output_file_png.split('/')[-1])
    if '_STARTTIME' in line:
        dd = str(datetime.datetime.strptime(best_product['properties']['startDate'], '%Y-%m-%dT%H:%M:%SZ'))
        line = line.replace('_STARTTIME', dd)
    if '_PROCTIME' in line:
        line = line.replace('_PROCTIME', str(duration))
    if '_PRODSIZE' in line:
        line = line.replace('_PRODSIZE', str(best_product['properties']['services']['download']['size']/(1024**2)))
    if '_PRODPATH' in line:
        line = line.replace('_PRODPATH', best_product['properties']['productIdentifier'])
    if '_CCOVERAGE' in line:
        line = line.replace('_CCOVERAGE', str(best_product['properties']['cloudCover']))
    return line


def put_s3_file(inputFileName):
    logging.info('START copying {0}'.format(inputFileName))
    try:
        ret = s3.upload_file(inputFileName, BUCKET, 'png/' + inputFileName.split('/')[-1])
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)
    return ret


def get_max_dir_name():
    logging.info('START');
    mypath = WWW_ROOT + '/grav/user/pages/02.eo_images/'
    dd = [int(f.split('.')[0]) for f in listdir(mypath) if not isfile(join(mypath, f))]
    return max(dd) + 1


def write_cms_files(best_product, output_file_png, duration):
    logging.info('START');
    maxnum = str(get_max_dir_name())
    now = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        dirName = WWW_ROOT + '/grav/user/pages/02.eo_images/' + maxnum + '.EO_' + maxnum
        os.mkdir(dirName)
        # Create the new config file for writing
        config = io.open(dirName + '/item.md', 'w')
        logging.info('Copying into: {0}'.format(dirName + '/item.md'))
        # Read the lines from the template, substitute the values, and write to the new config file
        for line in io.open(TEMPLATE, "r", encoding="utf-8"):
            config.write(line_tune(line, best_product, output_file_png, duration, maxnum, now))
        # Close the files
        config.close()
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)

    try:
        config = io.open(WWW_ROOT + '/grav/user/pages/01.home/default.md', 'w')
        logging.info('Copying into: {0}'.format(WWW_ROOT + '/grav/user/pages/01.home/default.md'))
        for line in io.open(TEMPLATE_DEFAULT, "r", encoding="utf-8"):
            config.write(line_tune(line, best_product, output_file_png, duration, maxnum, now))
        config.close()
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)
    publish_kpi_data(now, duration, best_product, output_file_png);



def put_to_objectstore():
    # TODO
    pass


def update_index():
    # TODO
    pass


def clean_up():
    # TODO
    pass


def publish_kpi_data(now, duration, best_product, output_file_png):
    logging.info('START')
    logging.info('Writing to {0}'.format(WWW_ROOT + RESULTS))
    stw = \
        now + '\n' + str(duration) + '\n' + str(0) + '\n' + best_product['properties']['productIdentifier'] \
        + '\n' + S3_ENDPOINT + '/swift/v1/' + BUCKET + '/png/' + output_file_png.split('/')[-1] + '\n'
    with open(WWW_ROOT + RESULTS, 'w') as the_file:
        the_file.write(stw)
    s3.upload_file(WWW_ROOT + RESULTS, BUCKET, 'png' + RESULTS)


def clear():
    from shutil import rmtree
    try:
        mypath = HOME + '/.snap/var/cache/s2tbx/l1c-reader/6.0.0/'
        for i in listdir(mypath):
            rmtree(mypath + i)
    except Exception as e:
        logging.error(str(e))
    try:
        for i in listdir(WORK_DIR):
            if i[:2] == 'S2':
                rmtree(WORK_DIR + '/' + i)
    except Exception as e:
        logging.error(str(e))


def job():
    logging.info('START')
    try:
        start_time = time.time()
        products = find_products(FINDER_API_URL)
        best_product = select_product(products)
        output_file_png = convert_product(best_product)
        # push = put_s3_file(output_file_png)
        put_s3_file(output_file_png)
        duration = round(time.time() - start_time, 2)
        write_cms_files(best_product, output_file_png, duration)
    except Exception as e:
        logging.error(str(e))
    # clear()
    logging.info('STOP')


if __name__ == '__main__':

    logging.basicConfig(filename=WORK_DIR + '/s2scenes.log', level=logging.DEBUG,
                        format='%(asctime)s - %(funcName)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

    with open(WORK_DIR + '/.bench-front.cfg') as cfgFile:
        lines = cfgFile.readlines()
        aaki = lines[0].rstrip()
        asak = lines[1].rstrip()
        eu = lines[2].rstrip()

    s3 = boto3.client('s3', aws_access_key_id=aaki, aws_secret_access_key=asak, endpoint_url=eu)

    stopper1 = Timer()

    job()

    stopper1.stop()
    print("Duration: {0}".format(stopper1.duration))
