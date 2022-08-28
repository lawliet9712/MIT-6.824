#!/bin/bash
for ((i=1; i<=100; i++)); do
go test -race  -run $1 >> test.log
done
