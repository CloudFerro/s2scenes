#!/usr/bin/env python3

import datetime
import requests
import json
import sys
import logging
import boto3
import os
import io
from timer import Timer
from subprocess import Popen, PIPE
from os import listdir
from os.path import isfile, join
from shutil import rmtree

# Set TEST = True for testing purpose, without actual product conversion.

TEST = False
TEST_PRD = 'S2B_MSIL1C_20181228T111449_N0207_R137_T30STE_20181228T131040.SAFE'

# Basic settings
# NUM_OF_PRD: number of products to choose from
# CLOUD_COVERAGE: max cloud coverage (percent)
# *_SIZE: SIZE of products to choose from

NUM_OF_PRD = 2000
CLOUD_COVERAGE = 20
ALL_TIME_AVG_SIZE = 629145600
MAX_PROD_SIZE = 1029145600
MIN_PROD_SIZE = 729145600

# Search starting from START_DATE

START_DATE = (datetime.datetime.now() + datetime.timedelta(days=-30)).isoformat()

# Finder API URL

FINDER_API_URL = 'https://finder.eocloud.eu/resto/api/collections/Sentinel2/search.json?\
            maxRecords={0}&\
            processingLevel=LEVELL1C&\
            publishedAfter={1}&\
            cloudCover=[0,{2}]&\
            sortParam=startDate&sortOrder=descending&\
            dataset=ESA-DATASET'.format(NUM_OF_PRD, START_DATE, CLOUD_COVERAGE)

# Local folders and file placement

HOME = os.environ['HOME']
WORK_DIR = HOME + '/bench-front'
TEMPLATE = WORK_DIR + '/template.md'
TEMPLATE_DEFAULT = WORK_DIR + '/template_default.md'
RESULTS = '/results.out'

# Location of WWW root, where Grav folder is placed

WWW_ROOT = '/var/www/html'

# S3 endpoint and bucket

S3_ENDPOINT = 'https://eocloud.eu:8080'
BUCKET = 'front-office-sample'

# ESA snap pconvert executable

PCONVERT = '/usr/local/snap/bin/pconvert'

# Log file

LOG = WORK_DIR + '/s2scenes.log'

# Logging configuration

logging.basicConfig(filename=WORK_DIR + '/s2scenes.log', level=logging.INFO,
                        format='%(asctime)s %(filename)s: %(funcName)s (%(lineno)d): %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

if TEST:
    FINDER_API_URL = 'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?' \
                     'maxRecords=10&' \
                     'productIdentifier=%{0}%&' \
                     'sortParam=startDate&' \
                     'sortOrder=descending&' \
                     'status=all&' \
                     'dataset=ESA-DATASET'.format(TEST_PRD)


# Search for products in EO Finder
def find_products(url: str):
    try:
        logging.info('START')
        logging.info('Finder URL: ' + url.replace(' ', ''))
        r = requests.get(url)
        logging.info('Found ' + str(len(r.json()['features'])) + ' products')
        logging.info('STOP')
    except Exception as e:
        logging.error('ERROR: ' + str(e))
        logging.info('STOP')
    return r.json()


# Select random product to process
def select_product(products: json):
    logging.info('START')
    from random import randint
    if not products:
        logging.error('ERROR: No products provided for select_product')
        logging.info('STOP')
        sys.exit(1)

    prods_narrow_selection = [p for p in products['features']
                              if (MAX_PROD_SIZE >= p['properties']['services']['download']['size'] >= MIN_PROD_SIZE)]

    logging.info('Narrow selection: ' + str(len(prods_narrow_selection)))

    r = randint(0, len(prods_narrow_selection) - 1)
    _product = prods_narrow_selection[r]

    logging.info('Selected product: ' + _product['properties']['title'])
    logging.info('STOP')

    return _product


# Convert selected product
def convert_product(_product: json):
    logging.info('START')
    if TEST:
        logging.info('STOP')
        return WORK_DIR + '/' + TEST_PRD + '/' + TEST_PRD.split(sep='.')[0] + '.png'
    else:
        if not _product:
            logging.error('ERROR: No product provided for convert_product')
            logging.info('STOP')
            sys.exit(1)
        try:
            os.mkdir('{0}/{1}/'.format(WORK_DIR, _product['properties']['title']))
        except Exception as e:
            logging.error('ERROR: Failed on mkdir {0}/{1}/. With error: {2}'
                          .format(WORK_DIR, _product['properties']['title'], str(e)))
            logging.info('STOP')
            sys.exit(1)

        comm = PCONVERT + ' -f png -W 800 -p {0}/rgb_def.txt -o {0}/{1}/ {2}'\
            .format(WORK_DIR, _product['properties']['title'], _product['properties']['productIdentifier'])
        p = Popen(comm.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False, universal_newlines=True)
        (output, err) = p.communicate()
        logging.info(output)
        logging.info(err)
        logging.info('STOP')
        return output.splitlines()[-1].split()[-1][1:-4]


# Template file parser
def _line_tune(line, best_product, output_file_png, duration, maxnum, now):
    if '_NOW' in line:
        line = line.replace('_NOW', now)
    if '_TAGS' in line:
        tags = ', '.join([x['name'].replace("'", "") for x in best_product['properties']['keywords']
                          if x['name'][0] != '_'][:-6])
        line = line.replace('_TAGS', tags)
    if '_MAXNUM' in line:
        line = line.replace('_MAXNUM', maxnum)
    if '_PRODNAME' in line:
        line = line.replace('_PRODNAME', best_product['properties']['title'])
    if '_PRODURL' in line:
        line = line.replace('_PRODURL', S3_ENDPOINT + "/swift/v1/" + BUCKET + "/png/" + output_file_png.split('/')[-1])
    if '_STARTTIME' in line:
        dd = str(datetime.datetime.strptime(best_product['properties']['startDate'], '%Y-%m-%dT%H:%M:%S.%fZ'))
        line = line.replace('_STARTTIME', dd)
    if '_PROCTIME' in line:
        line = line.replace('_PROCTIME', str(duration))
    if '_PRODSIZE' in line:
        line = line.replace('_PRODSIZE', str(round(best_product['properties']['services']['download']['size'] / (1024 ** 2), 2)))
    if '_PRODPATH' in line:
        line = line.replace('_PRODPATH', best_product['properties']['productIdentifier'])
    if '_CCOVERAGE' in line:
        line = line.replace('_CCOVERAGE', str(round(best_product['properties']['cloudCover'], 2)))
    return line


# Upload processed PNG to S3
def put_s3_file(input_filename):
    logging.info('START')
    if TEST:
        logging.info('STOP')
        return
    else:
        logging.info('START copying {0}'.format(input_filename))
        try:
            ret = s3.upload_file(input_filename, BUCKET, 'png/' + input_filename.split('/')[-1])
            return ret
        except Exception as e:
            logging.error('ERROR: ' + str(e))
        logging.info('STOP')
        return


# Find highest number of directory in /grav/user/pages/02.eo_images/
def _get_max_dir_name():
    logging.info('START')
    my_path = WWW_ROOT + '/grav/user/pages/02.eo_images/'
    dd = [int(f.split('.')[0]) for f in listdir(my_path) if not isfile(join(my_path, f))]
    logging.info('max_dir_name = {}'.format(max(dd)))
    logging.info('STOP')
    return max(dd) + 1


# Update CMS
def write_cms_files(best_product, output_file_png, duration):
    logging.info('START')
    maxnum = str(_get_max_dir_name())
    now = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        dir_name = WWW_ROOT + '/grav/user/pages/02.eo_images/' + maxnum + '.EO_' + maxnum
        os.mkdir(dir_name)
        # Create the new config file for writing
        config = io.open(dir_name + '/item.md', 'w')
        logging.info('Copying into: {0}'.format(dir_name + '/item.md'))
        # Read the lines from the template, substitute the values, and write to the new config file
        for line in io.open(TEMPLATE, "r", encoding="utf-8"):
            config.write(_line_tune(line, best_product, output_file_png, duration, maxnum, now))
        # Close the files
        config.close()
    except Exception as e:
        logging.error('ERROR:' + str(e))
        logging.info('STOP')
        sys.exit(1)

    try:
        config = io.open(WWW_ROOT + '/grav/user/pages/01.home/default.md', 'w')
        logging.info('Copying into: {0}'.format(WWW_ROOT + '/grav/user/pages/01.home/default.md'))
        for line in io.open(TEMPLATE_DEFAULT, "r", encoding="utf-8"):
            config.write(_line_tune(line, best_product, output_file_png, duration, maxnum, now))
        config.close()
    except Exception as e:
        logging.error('ERROR: ' + str(e))
        logging.info('STOP')
        sys.exit(1)
    publish_kpi_data(now, duration, best_product, output_file_png)
    logging.info('STOP')


# Publish KPI data
def publish_kpi_data(now, duration, best_product, output_file_png):
    logging.info('START')
    logging.info('Writing to {0}'.format(WWW_ROOT + RESULTS))
    stw = \
        now + '\n' + str(duration) + '\n' + str(0) + '\n' + best_product['properties']['productIdentifier'] \
        + '\n' + S3_ENDPOINT + '/swift/v1/' + BUCKET + '/png/' + output_file_png.split('/')[-1] + '\n'
    with open(WWW_ROOT + RESULTS, 'w') as the_file:
        the_file.write(stw)
    s3.upload_file(WWW_ROOT + RESULTS, BUCKET, 'png' + RESULTS)
    logging.info('STOP')


# Cleanup after processing
def clear():
    logging.info('START')
    try:
        my_path = HOME + '/.snap/var/cache/s2tbx/l1c-reader/6.0.0/'
        for i in listdir(my_path):
            logging.info('Deleting: {}'.format(my_path + i))
            rmtree(my_path + i)
    except Exception as e:
        logging.error('ERROR: ' + str(e))
    try:
        for i in listdir(WORK_DIR):
            if i != TEST_PRD:
                if i[:2] == 'S2':
                    logging.info('Deleting: {}'.format(WORK_DIR + '/' + i))
                    rmtree(WORK_DIR + '/' + i)
    except Exception as e:
        logging.error('ERROR: ' + str(e))
    logging.info('STOP')


# Main processing job
def job():

    logging.info('START')
    stoppers = []
    stopper_main = Timer()

    try:

        stopper_finder = Timer()
        products = find_products(FINDER_API_URL)
        best_product = select_product(products)
        stopper_finder.stop()
        stoppers.append('finder = {}'.format(stopper_finder.duration))

        stopper_convert = Timer()
        output_file_png = convert_product(best_product)
        stopper_convert.stop()
        stoppers.append('convert = {}'.format(stopper_convert.duration))

        stopper_s3 = Timer()
        put_s3_file(output_file_png)
        stopper_s3.stop()
        stoppers.append('s3 = {}'.format(stopper_s3.duration))

        stopper_cms = Timer()
        duration = round(stopper_convert.duration, 2)
        write_cms_files(best_product, output_file_png, duration)
        stopper_cms.stop()
        stoppers.append('cms = {}'.format(stopper_cms.duration))

    except Exception as e:
        logging.error('ERROR: ' + str(e))

    stopper_clear = Timer()
    clear()
    stopper_clear.stop()
    stoppers.append('clear = {}'.format(stopper_clear.duration))

    stopper_main.stop()
    stoppers.append('main = {}'.format(stopper_main.duration))

    logging.info('Stoppers' + str([s for s in stoppers]))

    logging.info('STOP')


if __name__ == '__main__':

    logging.info('START_PROCESSING')

    with open(WORK_DIR + '/.bench-front.cfg') as cfg_file:
        lines = cfg_file.readlines()
        aaki = lines[0].rstrip()
        asak = lines[1].rstrip()
        eu = lines[2].rstrip()

    s3 = boto3.client('s3', aws_access_key_id=aaki, aws_secret_access_key=asak, endpoint_url=eu)

    job()

    logging.info('END_PROCESSING')
