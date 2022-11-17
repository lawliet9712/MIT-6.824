#!/bin/bash
# Author: wanghan
# Created Time : Tue 17 Jul 2018 07:35:49 PM CST
# File Name: init.sh
# Description:

function loop_exe()
{
    local ex_count=0
    CMDLINE=$1
    while true ; do
        #command
        sleep 1
        echo The command is \"$CMDLINE\"
        ${CMDLINE}
        if [ $? == 0 ] ; then
            (( ex_count = ${ex_count} + 1 ))
            echo The command execute OK!
        else
            echo ERROR : The command execute fialed! ex_count = ${ex_count}.
            break;
        fi
    done
}


function main()
{
    echo --- Start ---
    loop_exe "sh ./test_wrapper.sh"
    echo --- Done ---
}

main
