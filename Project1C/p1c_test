#!/bin/bash
TMP_DIR=/tmp/p1c_tmp
REQUIRED_TXT_FILES="readme.txt team.txt"
REQUIRED_P1B_FILES="create.sql load.sql"
REQUIRED_TEST_FILES="t1.html t2.html t3.html t4.html t5.html"

# usage
if [ $# -ne 1 ]
then
     echo "Usage: $0 Your_UID" 1>&2
     exit
fi

ZIP_FILE="P1C.zip"
FOLDER_NAME=$1

# clean any existing files
rm -rf ${TMP_DIR}
mkdir ${TMP_DIR}

# unzip the submission zip file 
if [ ! -f ${ZIP_FILE} ]; then
    echo "ERROR: Cannot find $ZIP_FILE, ensure this script is put in the same directory of your P1C.zip file. Otherwise check the zip file name" 1>&2
    rm -rf ${TMP_DIR}
    echo "rmd"
    exit 1
fi
unzip -q -d ${TMP_DIR} ${ZIP_FILE}
if [ "$?" -ne "0" ]; then 
    echo "ERROR: Cannot unzip ${ZIP_FILE} to ${TMP_DIR}"
    rm -rf ${TMP_DIR}
    exit 1
fi

# change directory to the grading folder
cd ${TMP_DIR}

if [ ! -d ${FOLDER_NAME} ];
then
echo "Check your folder name is EXACTLY the same as UID you typed"
rm -rf ${TMP_DIR}
exit 1
fi

cd ${FOLDER_NAME}

for FILE in ${REQUIRED_TXT_FILES}
do
    if [ ! -f ${FILE} ]; then
        echo "ERROR: Cannot find ${FILE} in your zip file" 1>&2
	rm -rf ${TMP_DIR}
        exit 1
    fi
done

if [ ! -d www ];
then
echo "Lack www folder"
rm -rf ${TMP_DIR}
exit 1
fi

cd www

if [[ -n $(ls) ]]; then
    :
else
    echo "www folder should contain at least one file or directory"
    rm -rf ${TMP_DIR}
    exit 1
fi

cd ..

if [ ! -d sql ];
then 
echo "Lack sql folder"
rm -rf ${TMP_DIR}
exit 1
fi

cd sql

# check the existence of the required files
for FILE in ${REQUIRED_P1B_FILES}
do
    if [ ! -f ${FILE} ]; then
        echo "ERROR: Cannot find ${FILE} in the sql folder of your zip file" 1>&2
	rm -rf ${TMP_DIR}
        exit 1
    fi
done

cd ..

if [ ! -d testcase ];
then 
echo "Lack testcase folder"
rm -rf ${TMP_DIR}
exit 1
fi

cd testcase

# check the existence of the required files
for FILE in ${REQUIRED_TEST_FILES}
do
    if [ ! -f ${FILE} ]; then
        echo "ERROR: Cannot find ${FILE} in the testcase folder of your zip file" 1>&2
	rm -rf ${TMP_DIR}
        exit 1
    fi
done

cd ..

echo "Check File Successfully. Please upload your P1C.zip file to CCLE."
rm -rf ${TMP_DIR}
exit 0