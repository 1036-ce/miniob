#!/bin/bash

cd test/case

python3 ./miniob_test.py --test-cases=primary-drop-table,basic,primary-date,primary-pure-null,primary-group-by,primary-null,primary-update,primary-expression,primary-aggregation-func,primary-join-tables,dblab-hash-join,primary-simple-sub-query,primary-complex-sub-query,primary-order-by,primary-text,primary-multi-index
# python3 ./miniob_test.py --test-cases=primary-multi-index

