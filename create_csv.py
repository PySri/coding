#! /usr/bin/env python

# This test file creates fake csv file with random files

import random
import sys
import re

ROWS = 1000
COL  = 100
csv_file = 'testfile.csv'
MAXVAL = sys.maxint

column_prefix = 'C'

temp_s = ''
for i in range(1,COL+1):
    if i == COL :
        temp_s = temp_s + column_prefix + str(i)  
    else:
        temp_s = temp_s + column_prefix + str(i) + ','

temp_s += '\n'

with open(csv_file,'w') as f:
    f.write(temp_s)

    for i in range(0,ROWS):
        row_l = []

        for x in range(1,COL+1):
            row_l.append(random.randint(-10000000,MAXVAL))

        temp_str = str(tuple(row_l))

        p1 = r'\('
        p2 = r'\)'
        p3 = r' '

        s1 = re.sub(p1,'',temp_str)
        s2 = re.sub(p2,'',s1)
        s3 = re.sub(p3,'',s2)
        s3 += '\n'

        f.write(s3)
