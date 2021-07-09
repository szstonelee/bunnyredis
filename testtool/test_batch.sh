# arg1 is the server ip address to test. arg2 is the loop times
for i in $(seq "$2")
do
echo "loop index = " $i
python3 test_string.py $1
python3 test_hash.py $1
python3 test_db.py $1
python3 test_transaction.py $1
done
exit 0
