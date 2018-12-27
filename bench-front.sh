#!/bin/bash

####################################################
# Initial configuration
## Collection name
COLLECTION=Sentinel2
## Search last 30 days
START_DATE=$(date --date="30 days ago" --iso-8601='seconds' | sed 's/\+02:00//g')
echo START_DATE: ${START_DATE}
## Number of products to return
NUM_OF_PRD=500
## All time average size of a single product in the collection
ALL_TIME_AVG_SIZE=629145600
MAX_PROD_SIZE=1029145600
## Temp files
TMP_FILE=/tmp/products_tmp_$(date +%s).json
TMP_NR=/tmp/nr_tmp_$(date +%s)
## Index file
INDEX=~/bench-front/item.md
## Template file
TEMPLATE=~/bench-front/template.md
## KPI results data
RESULTS=~/bench-front/results.out

EXIT_CODE=0

## Error handling
function handle_exit () {

    EXIT_CODE=${?}
    if [ ${EXIT_CODE} -ne 0 ];
    then
        echo ERROR: Function: ${1} FAILED, code ${EXIT_CODE};
    else
        echo INFO: Function: ${1} OK, code ${EXIT_CODE};
    fi
}


## Start timer
function start_timer () {

    T1=$(date +%s)
    NOW=$(date)
    _NOW=$(date --iso-8601='seconds' | sed 's/\+02:00//g; s/T/ /g')
}

## Authorize in OpenStack
function auth_in_openstack () {

    FUNC_NAME=${FUNCNAME[0]}
    source cloud_01032_project_with_eo-openrc.sh
    handle_exit ${FUNC_NAME}
}

## Get products from EO Finder API
function find_products () {

    FUNC_NAME=${FUNCNAME[0]}
    # Skip execution if previous function failed
    if [ ${EXIT_CODE} -ne 0 ]; then echo ERROR: Skipping ${FUNC_NAME} cause previous function failed; return 1; fi
    ## By startDate
    # curl -o ${TMP_FILE} "https://finder.eocloud.eu/resto/api/collections/${COLLECTION}/search.json?_pretty=true&maxRecords=${NUM_OF_PRD}&startDate=${START_DATE}&sortParam=startDate&sortOrder=ascending" 2> /dev/null
    ## By publishedDate
    echo INFO: TMP_FILE: ${TMP_FILE}
    # curl -g -o ${TMP_FILE} "https://finder.creodias.eu/resto/api/collections/${COLLECTION}/search.json?_pretty=true&maxRecords=${NUM_OF_PRD}&publishedAfter=${START_DATE}&cloudCover=[0,20]&sortParam=startDate&sortOrder=descending&dataset=ESA-DATASET" 2> /dev/null
    ## Added condition: &processingLevel=LEVELL1C in order to eliminate L2 which is available only for ordering but not in repository
    # curl -g -o ${TMP_FILE} "https://finder.creodias.eu/resto/api/collections/${COLLECTION}/search.json?_pretty=true&maxRecords=${NUM_OF_PRD}&processingLevel=LEVELL1C&publishedAfter=${START_DATE}&cloudCover=[0,20]&sortParam=startDate&sortOrder=descending&dataset=ESA-DATASET" 2> /dev/null

    curl -g -o ${TMP_FILE} "https://finder.creodias.eu/resto/api/collections/${COLLECTION}/search.json?_pretty=true&maxRecords=${NUM_OF_PRD}&processingLevel=LEVEL1C&publishedAfter=${START_DATE}&cloudCover=[0,20]&sortParam=startDate&sortOrder=descending&status=0|34&dataset=ESA-DATASET" 2> /dev/null

    handle_exit ${FUNC_NAME}
}


## Select product
## This function is quite complex only because it has to search
## for a product that size is closest to the ALL_TIME_AVG_SIZE.
## Otherwise it would be simple.
function select_product () {

    FUNC_NAME=${FUNCNAME[0]}
    # Skip execution if previous function failed
    if [ ${EXIT_CODE} -ne 0 ]; then echo ERROR: Skipping ${FUNC_NAME} cause previous function failed; return 1; fi
    ## Method 1:
    ## Select 10 random products from $NUM_OF_PRD and
    ## find size of a product closest to All time average
    # for v in `cat ${TMP_FILE} | grep size | awk '{print $2}' | shuf -n 10`;
    #     do echo "a=($v-629145600);if(0>a)a*=-1;a" | bc | tr '\n' ' '; echo $v;
    # done | sort -n | head -1 | awk '{print $2}' > ${TMP_NR}
    # export SIZE=$(cat ${TMP_NR})

    # echo INFO: SIZE: ${SIZE}

    ## Method 2:
    ## Find random product
    # cat ${TMP_FILE} | grep size | awk '{print $2}' | shuf -n 1 > ${TMP_NR}
    # cat ${TMP_NR}
    # export SIZE=$(cat ${TMP_NR})

    ## Find path
    ## By size of product
    # P_PATH=$(jq -r --arg SIZE "${SIZE}" ".features[] | select (.properties.services.download.size == ${SIZE}) | .properties.productIdentifier" ${TMP_FILE})
    # N_PATH=$(jq -r --arg SIZE "${SIZE}" ".features[] | select (.properties.services.download.size == ${SIZE}) | .properties.title" ${TMP_FILE})
    # F_NAME=$(echo ${N_PATH} | sed 's/\.SAFE/\.png/g')
    ## By size > $ALL_TIME_AVG_SIZE and cloudCover < 30%
    PROD_ID=$(jq -r --arg ALL_TIME_AVG_SIZE "${ALL_TIME_AVG_SIZE}" ".features[] | select ((.properties.services.download.size > ${ALL_TIME_AVG_SIZE}) and (.properties.services.download.size < 1029145600)) | .id" ${TMP_FILE} | shuf -n 1)
    echo INFO: PROD_ID: ${PROD_ID}

    P_PATH=$(jq -r --arg PROD_ID "${PROD_ID}" ".features[] | select (.id == \"${PROD_ID}\") | .properties.productIdentifier" ${TMP_FILE} | head -1)
    N_PATH=$(jq -r --arg PROD_ID "${PROD_ID}" ".features[] | select (.id == \"${PROD_ID}\") | .properties.title" ${TMP_FILE} | head -1)
    SIZE=$(jq -r --arg PROD_ID "${PROD_ID}" ".features[] | select (.id == \"${PROD_ID}\") | .properties.services.download.size" ${TMP_FILE} | head -1)
    F_NAME=$(echo ${N_PATH} | sed 's/\.SAFE$/\.png/g')


    echo INFO: PATH P_PATH: ${P_PATH}
    echo INFO: NAME N_PATH: ${N_PATH}
    echo INFO: FILE EXPECTED F_NAME: ${F_NAME}
    echo INFO: SIZE: ${SIZE}


    handle_exit ${FUNC_NAME}
}


## Convert product using snap tool: pconvert
function convert_product () {

    FUNC_NAME=${FUNCNAME[0]}
    # Skip execution if previous function failed
    if [ ${EXIT_CODE} -ne 0 ]; then echo ERROR: Skipping ${FUNC_NAME} cause previous function failed; return 1; fi
    mkdir ${N_PATH}
    # /usr/local/snap/bin/pconvert -f png -W 800 -p rgb_def.txt -o ${P_PATH} ${P_PATH}

    cp -R ${P_PATH} /tmp/

    /usr/local/snap/bin/pconvert -f png -W 800 -p ~/bench-front/rgb_def.txt -o ./${N_PATH} /tmp/${N_PATH}

    handle_exit ${FUNC_NAME}

    F_NAME=$(ls -1 ./${N_PATH})
    echo INFO: FILE ACTUAL F_NAME: ${F_NAME}
}


## Put to object store
function put_to_object_store () {

    FUNC_NAME=${FUNCNAME[0]}
    # Skip execution if previous function failed
    if [ ${EXIT_CODE} -ne 0 ]; then echo ERROR: Skipping ${FUNC_NAME} cause previous function failed; return 1; fi
    /usr/local/bin/s3cmd put ${N_PATH}/${F_NAME} s3://front-office-sample/png/
    handle_exit ${FUNC_NAME}
}


## End timer
function end_timer () {

    T2=$(date +%s)
    DURATION=$(expr ${T2} - ${T1})
}


## Update index
function update_index () {

    FUNC_NAME=${FUNCNAME[0]}
    # Skip execution if previous function failed
    if [ ${EXIT_CODE} -ne 0 ]; then echo ERROR: Skipping ${FUNC_NAME} cause previous function failed; return 1; fi
    _TAGS=$(jq --arg SIZE "${SIZE}" ".features[] | select (.properties.services.download.size == ${SIZE}) | .properties.keywords[].name" ${TMP_FILE} |
            grep -v all | sed 's/"//g' | head -n -6 | sed ':a;N;$!ba;s/\n/, /g' | sed "s/'//g")
    _MAXNUM=$(expr $(ls -f1 /var/www/html/grav/user/pages/02.eo_images/ | awk -F'.' '{print $1}' | grep -v blog | sort -V | tail -1) + 1)
    _PRODNAME=${N_PATH}
    _PRODURL="https://eocloud.eu:8080/swift/v1/front-office-sample/png/${F_NAME}"
    _STARTTIME=$(jq -r --arg SIZE "${SIZE}" ".features[] | select (.properties.services.download.size == ${SIZE}) | .properties.startDate" ${TMP_FILE} | sed 's/[T,Z]/ /g' | head -1)
    _PROCTIME=${DURATION}
    _PRODSIZE=$(echo "${SIZE} / 1024^2" | bc)
    _PRODPATH=${P_PATH}
    _CCOVERAGE=$(jq -r --arg SIZE "${SIZE}" ".features[] | select (.properties.services.download.size == ${SIZE}) | .properties.cloudCover" ${TMP_FILE} | head -1)

    cat ${TEMPLATE} | sed "s/_NOW/${_NOW}/g; s/_MAXNUM/${_MAXNUM}/g; s/_PRODNAME/${_PRODNAME}/g; s/_STARTTIME/${_STARTTIME}/g; s/_PROCTIME/${_PROCTIME}/g; s/_PRODSIZE/${_PRODSIZE}/g; s/_CCOVERAGE/${_CCOVERAGE}/g; s/_TAGS/${_TAGS}/g" > /tmp/temp1.md
    cat /tmp/temp1.md | sed "s~_PRODURL~${_PRODURL}~g; s~_PRODPATH~${_PRODPATH}~g" > ${INDEX}

    # echo "=================== CONTENT OF INDEX MARKUP (START) ==================="
    # cat ${INDEX}
    # echo "=================== CONTENT OF INDEX MARKUP (END) ==================="

    mkdir /var/www/html/grav/user/pages/02.eo_images/${_MAXNUM}.EO_${_MAXNUM}
    cp ${INDEX} /var/www/html/grav/user/pages/02.eo_images/${_MAXNUM}.EO_${_MAXNUM}/

    head -12 /var/www/html/grav/user/pages/01.home/default.md > /tmp/default1.md
    cat /var/www/html/grav/user/pages/02.eo_images/${_MAXNUM}.EO_${_MAXNUM}/item.md | tail -n -20 >> /tmp/default1.md
    cat /tmp/default1.md > /var/www/html/grav/user/pages/01.home/default.md

    # echo "${NOW}: Conversion of <a href=\"https://eocloud.eu:8080/swift/v1/front-office-sample/png/${F_NAME}\">${F_NAME}</a> took ${DURATION} seconds.<br>" >> ${INDEX}
    handle_exit ${FUNC_NAME}
}


## Clean up
function clean_up () {

#    rm ${TMP_FILE} ${TMP_NR} ${N_PATH}/${F_NAME}
    rm ${TMP_FILE} ${N_PATH}/${F_NAME}
    if [ -d /tmp/${N_PATH} ]; then
        chmod -R u+w /tmp/${N_PATH}
        rm -rf /tmp/${N_PATH}
    fi
    rm /tmp/temp1.md
    rmdir ${N_PATH}
    ## Clean snap cache garbage
    rm -rf ~/.snap/var/cache/s2tbx/l1c-reader/6.0.0/*
}


## Publish KPI data
function publish_kpi_data () {

    TIMESTAMP=${NOW}
    RESULT=${DURATION}
    EXIT_CODE=0	# To be developed
    PRODUCT_ID=${P_PATH}
    URL="https://eocloud.eu:8080/swift/v1/front-office-sample/png/${F_NAME}"

    echo ${TIMESTAMP} > ${RESULTS}
    echo ${RESULT} >> ${RESULTS}
    echo ${EXIT_CODE} >> ${RESULTS}
    echo ${PRODUCT_ID} >> ${RESULTS}
    echo ${URL} >> ${RESULTS}

    /bin/cp ${RESULTS} /var/www/html/
    /usr/local/bin/s3cmd put ${RESULTS} s3://front-office-sample/png/
}


## Execute sequence of tasks

start_timer
# auth_in_openstack
find_products
select_product
convert_product
put_to_object_store
end_timer
update_index
clean_up
publish_kpi_data
