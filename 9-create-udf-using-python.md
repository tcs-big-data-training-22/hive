ssh u18@20.245.236.124
ls
nano udf_v1.py
```
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
```

beeline -u 'jdbc:hive2://headnodehost:10001/;transportMode=http'

ADD FILE /home/u18/udf_v1.py;

CREATE DATABASE tmp;
USE tmp;
CREATE TABLE foo (id INT, vtype STRING, price FLOAT);
INSERT INTO TABLE foo VALUES (1, "car", 1000.);
INSERT INTO TABLE foo VALUES (2, "car", 42.);
INSERT INTO TABLE foo VALUES (3, "car", 10000.);
INSERT INTO TABLE foo VALUES (4, "car", 69.);
INSERT INTO TABLE foo VALUES (5, "bike", 1426.);
INSERT INTO TABLE foo VALUES (6, "bike", 32.);
INSERT INTO TABLE foo VALUES (7, "bike", 1234.);

select * from foo;

SELECT TRANSFORM (id, vtype, price)
    USING 'python udf_v1.py' AS
    (id INT, vtype STRING, price FLOAT)
FROM tmp.foo;
;
