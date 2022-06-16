#!/usr/local/bin/python
import hashlib
import sys

## we are receiving each record passed in from Hive via standard input 
## By default, columns will be transformed to STRING and delimited by TAB 
## Also, by default, NULL values will be converted to literal string \N to differentiate from empty strings
for line in sys.stdin:
  line = line.strip()
  (id,vtype,price) = line.split('\t')
  price = float(price)*2
  ## hash social security number and emit all the fields to standard out
  print '\t'.join([str(id),str(vtype),str(price)])