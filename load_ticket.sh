#!/bin/bash
JIRA=$1
ES_HOST=$2
RELOAD=$3
FILTER=$4
PROJECTS=$5

source /home/spectrum/alhena_bccrc/venv/bin/activate
source /home/spectrum/alhena-loader/set_credentials

extraArgs=''
if $RELOAD; then
	alhena_bccrc \
        	--host $ES_HOST \
        	--id $JIRA \
        	clean
	#extraArgs="$extraArgs--reload "
fi

if $FILTER; then
	extraArgs="$extraArgs--filtered "
fi

alhena_bccrc \
	--host $ES_HOST \
	--id $JIRA \
	--dir "/dat/alhena" \
	download $extraArgs

alhena_bccrc \
	--host $ES_HOST \
	--id $JIRA \
	--dir "/dat/alhena" \
	load $PROJECTS

#echo "python /home/spectrum/alhena-loader/alhena_cli.py --host ${ES_HOST} load-analysis-shah --id ${JIRA} /dat/alhena/${JIRA} --download ${PROJECTS} ${extraArgs}"
