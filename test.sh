#!/bin/bash

cd test/case

python3 ./miniob_test.py --test-cases=primary-drop-table,basic,primary-date,primary-pure-null,primary-group-by,primary-null,primary-update,primary-expression,primary-aggregation-func,primary-join-tables,dblab-hash-join,primary-simple-sub-query,primary-complex-sub-query,primary-order-by,primary-text,primary-multi-index,primary-aggregation-and-groupby,primary-new-join,primary-new-csq,primary-like,primary-alias,primary-unique,primary-new-unique
# python3 ./miniob_test.py --test-cases=primary-text
