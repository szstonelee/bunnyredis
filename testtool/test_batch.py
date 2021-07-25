import sys
import subprocess


def _main():
    r_ip = str(sys.argv[1])
    r1_ip = str(sys.argv[2])
    r2_ip = str(sys.argv[3])
    test_times = int(sys.argv[4])

    for i in range(test_times):
        print("----------------------------------------------------------------------")
        print(f"test batch index = {i}")

        print("\t\t\t\t test_string.py")
        subprocess.check_call(["python3", "test_string.py", r_ip, r1_ip, r2_ip])
        print("\t\t\t\t test_hash.py")
        subprocess.check_call(["python3", "test_hash.py", r_ip, r1_ip, r2_ip])
        print("\t\t\t\t test_db.py")
        subprocess.check_call(["python3", "test_db.py", r_ip, r1_ip, r2_ip])
        print("\t\t\t\t test_transaction.py")
        subprocess.check_call(["python3", "test_transaction.py", r_ip, r1_ip, r2_ip])


if __name__ == '__main__':
    _main()