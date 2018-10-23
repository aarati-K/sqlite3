#!/bin/bash
cd /home/aarati/workspace/sqlite
git checkout -q master
make clean
make
cd experiments

declare -a arr=(50000 5000 500 50000)

for i in "${arr[@]}"
do
  echo "Starting experiment for num_inserts_in_transaction =" $i
  rm -f ../64kb_bad_layout.db && touch ../64kb_bad_layout.db
  g++ generate_bad_layout.cpp && ./a.out
  g++ benchmark_test.cpp ../libsqlite3.a -ldl -lpthread
  ./a.out $i
  echo "Finished experiment for num_inserts_in_transaction =" $i
done
