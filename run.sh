 
echo "s2 driver start thread 1 :"

./odbc_c_bench_dsnless   --driver-path /home/opc/singlestore-connector-odbc-1.1.7-centos8-amd64/libssodbcw.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 1 --arraysize 100000 --readonly

echo "s2 driver start thread 2 :"

./odbc_c_bench_dsnless   --driver-path /home/opc/singlestore-connector-odbc-1.1.7-centos8-amd64/libssodbcw.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 2 --arraysize 100000 --readonly


echo "mysql8 driver start thread 1 :"
./odbc_c_bench_dsnless   --driver-path /usr/lib64/libmyodbc8w.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 1 --arraysize 100000

echo "mysql8 driver start thread 2 :"
./odbc_c_bench_dsnless   --driver-path /usr/lib64/libmyodbc8w.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 2 --arraysize 100000
