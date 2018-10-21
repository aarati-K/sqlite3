import os
import matplotlib as plt
import numpy as np

DATA_FOLDER_BAD_LAYOUT = "/mnt/hdd/record/sqlite3/bad/"
THROUGHPUT_FOLDER_BAD_LAYOUT = "/mnt/hdd/record/sqlite3/bad/throughput/"

def extract_throughput_bad_layout():
  filenames = os.listdir(DATA_FOLDER_BAD_LAYOUT)
  filenames = filter(lambda fname: fname.endswith(".txt"), filenames)
  for f in filenames:
    infile = open(DATA_FOLDER_BAD_LAYOUT + f, 'r')
    outfile1 = open(THROUGHPUT_FOLDER_BAD_LAYOUT + "write/" + f , 'w')
    outfile2 = open(THROUGHPUT_FOLDER_BAD_LAYOUT + "read/" + f , 'w')
    index = 0
    for line in infile:
        t_usec = int(line)
        t_sec = float(t_usec)/1000000
        throughput = 50000/t_sec
        if index%2==0:
            outfile1.write(str(throughput)+"\n")
        else:
            outfile2.write(str(throughput)+"\n")
        index += 1

    if index != 200:
        print "Weird"

    infile.close()      
    outfile1.close()
    outfile2.close()

THROUGHPUT_FOLDER_CUM_BAD_LAYOUT = "/mnt/hdd/record/sqlite3/bad/throughput_cumulative/"

def extract_throughput_cum_bad_layout():
  filenames = os.listdir(DATA_FOLDER_BAD_LAYOUT)
  filenames = filter(lambda fname: fname.endswith(".txt"), filenames)
  for f in filenames:
    infile = open(DATA_FOLDER_BAD_LAYOUT + f, 'r')
    outfile1 = open(THROUGHPUT_FOLDER_CUM_BAD_LAYOUT + "write/" + f , 'w')
    outfile2 = open(THROUGHPUT_FOLDER_CUM_BAD_LAYOUT + "read/" + f , 'w')
    index = 0
    total_read_lat = 0
    total_write_lat = 0
    total_read_op = 0
    total_write_op = 0
    for line in infile:
        t_usec = int(line)
        t_sec = float(t_usec)/1000000
        if index%2==0:
            total_write_lat += t_sec
            total_write_op += 50000
            throughput = total_write_op/total_write_lat
            outfile1.write(str(throughput)+"\n")
        else:
            total_read_lat += t_sec
            total_read_op += 50000
            throughput = total_read_op/total_read_lat
            outfile2.write(str(throughput)+"\n")
        index += 1

    if index != 200:
        print "Weird"

    infile.close()      
    outfile1.close()
    outfile2.close()

DATA_FOLDER_GOOD_LAYOUT = "/mnt/hdd/record/sqlite3/good/"
THROUGHPUT_FOLDER_GOOD_LAYOUT = "/mnt/hdd/record/sqlite3/good/throughput/"

def extract_throughput_good_layout():
  filenames = os.listdir(DATA_FOLDER_GOOD_LAYOUT)
  filenames = filter(lambda fname: fname.endswith(".txt"), filenames)
  for f in filenames:
    infile = open(DATA_FOLDER_GOOD_LAYOUT + f, 'r')
    outfile1 = open(THROUGHPUT_FOLDER_GOOD_LAYOUT + "write/" + f , 'w')
    outfile2 = open(THROUGHPUT_FOLDER_GOOD_LAYOUT + "read/" + f , 'w')
    index = 0
    for line in infile:
        t_usec = int(line)
        t_sec = float(t_usec)/1000000
        throughput = 50000/t_sec
        if index%2==0:
            outfile1.write(str(throughput)+"\n")
        else:
            outfile2.write(str(throughput)+"\n")
        index += 1

    if index != 200:
        print "Weird"

    infile.close()      
    outfile1.close()
    outfile2.close()

THROUGHPUT_FOLDER_CUM_GOOD_LAYOUT = "/mnt/hdd/record/sqlite3/good/throughput_cumulative/"

def extract_throughput_cum_good_layout():
  filenames = os.listdir(DATA_FOLDER_GOOD_LAYOUT)
  filenames = filter(lambda fname: fname.endswith(".txt"), filenames)
  for f in filenames:
    infile = open(DATA_FOLDER_GOOD_LAYOUT + f, 'r')
    outfile1 = open(THROUGHPUT_FOLDER_CUM_GOOD_LAYOUT + "write/" + f , 'w')
    outfile2 = open(THROUGHPUT_FOLDER_CUM_GOOD_LAYOUT + "read/" + f , 'w')
    index = 0
    total_read_lat = 0
    total_write_lat = 0
    total_read_op = 0
    total_write_op = 0
    for line in infile:
        t_usec = int(line)
        t_sec = float(t_usec)/1000000
        if index%2==0:
            total_write_lat += t_sec
            total_write_op += 50000
            throughput = total_write_op/total_write_lat
            outfile1.write(str(throughput)+"\n")
        else:
            total_read_lat += t_sec
            total_read_op += 50000
            throughput = total_read_op/total_read_lat
            outfile2.write(str(throughput)+"\n")
        index += 1

    if index != 200:
        print "Weird"

    infile.close()      
    outfile1.close()
    outfile2.close()

def get_x_labels():
    num_iter = 100
    step = 0.6/num_iter
    x_labels = []
    cursize = 1.2
    for x in range(num_iter):
        x_labels.append(cursize)
        cursize += step
    return x_labels

def get_y_values(filename):
    infile = open(filename, 'r')
    y_labels = []
    for line in infile:
        y_labels.append(float(line))
    infile.close()
    return y_labels

def generate_cumulative_throughput(y_values):
    total  = 0
    count = 0
    cumulative = []
    for i in range(len(y_values)):
        count += 1
        total += y_values[i]
        cumulative.append(total/count)

    if len(cumulative) != len(y_values):
        print "Weird"

    return cumulative

extract_throughput_good_layout()
extract_throughput_bad_layout()

x_labels = get_x_labels()
insert_transaction_size = 25
fname = THROUGHPUT_FOLDER_BAD_LAYOUT + "read/" + str(insert_transaction_size) + ".txt-read"
y_values = get_y_values(fname)
cumulative_y_values = generate_cumulative_throughput(y_values)
