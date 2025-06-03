#!/bin/bash

cd test/case

python3 ./miniob_test.py --test-cases=primary-drop-table,basic,primary-date,primary-pure-null,primary-group-by,primary-null,primary-update,primary-expression,primary-aggregation-func,primary-join-tables
# python3 ./miniob_test.py --test-cases=primary-join-tables
