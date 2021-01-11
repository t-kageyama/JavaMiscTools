#!/bin/sh
#
# insert record shell script.
# date: 2021/01/11
# author: Toru Kageyama <info@comona.co.jp>
#

MY_DIR_NAME=`dirname $0`
SHELL_SCRIP_DIR=`cd $MY_DIR_NAME;pwd`
cd $SHELL_SCRIP_DIR

VERSION=1.0.0-SNAPSHOT
PACKAGE_NAME=jp.co.comona.javamisc.sql
MAIN_CLASS=InsertRecord
JAR_NAME=../build/libs/JavaMiscTools-$VERSION-all.jar

java -cp $JAR_NAME $PACKAGE_NAME.$MAIN_CLASS $@