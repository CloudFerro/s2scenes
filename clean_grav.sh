#!/bin/bash

# Clean grav pages by moving all except last 100 pages to an archive.

cd /var/www/html/grav/user/pages/02.eo_images/

NUMBER_OF_PAGES=$(ls -1 | sort -V | head -n -100 | wc -l)
echo Cleaning ${NUMBER_OF_PAGES} pages.
for i in `ls -1 | sort -V | head -n -100`; do mv -f $i /var/www/html/archiwalne/; done
