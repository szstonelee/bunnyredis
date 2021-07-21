import sys
import subprocess


def _main():
    r_ip = str(sys.argv[1])
    r1_ip = str(sys.argv[2])
    r2_ip = str(sys.argv[3])
    test_times = int(sys.argv[4])

    for i in range(test_times):
        print(f"test batch index = {i}")

        subprocess.check_call(["python3", "test_string.py", r_ip, r1_ip, r2_ip])
        subprocess.check_call(["python3", "test_hash.py", r_ip, r1_ip, r2_ip])
        subprocess.check_call(["python3", "test_db.py", r_ip, r1_ip, r2_ip])

        print("----------------------------------------------------")


if __name__ == '__main__':
    _main()