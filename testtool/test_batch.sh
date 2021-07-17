# arg1 is the real redis server ip address to test.
# arg2 is the rocked bunny-redis,
# arg3 is the mem bunny-redis
# arg4 is the loop times
for i in $(seq "$4")
do
echo "loop index = " $i
python3 test_string.py $1 $2 $3
python3 test_hash.py $1 $2 $3
python3 test_db.py $1 $2 $3
python3 test_transaction.py $1 $2 $3
done
exit 0
