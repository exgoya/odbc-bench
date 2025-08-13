 
echo "s2 driver start thread 1 :"

./odbc_c_bench_dsnless   --driver-path ./lib/libssodbcw.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 1 --arraysize 100000 --readonly

echo "s2 driver start thread 2 :"

./odbc_c_bench_dsnless   --driver-path ./lib/libssodbcw.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 2 --arraysize 100000 --readonly


echo "mysql8 driver start thread 1 :"
./odbc_c_bench_dsnless   --driver-path ./lib/libmyodbc8w.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 1 --arraysize 100000 --readonly

echo "mysql8 driver start thread 2 :"
./odbc_c_bench_dsnless   --driver-path ./lib/libmyodbc8w.so   --server 10.0.0.92 --port 3306 --user root --pwd test --database test   --query "SELECT * FROM TABLE_B_HISTORY limit 5000000"   --threads 2 --arraysize 100000 --readonly
