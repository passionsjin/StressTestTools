import subprocess

ls_output1 = subprocess.Popen(["python", "target.py", "1"])
ls_output2 = subprocess.Popen(["python", "target.py", "2"])
ls_output3 = subprocess.Popen(["python", "target.py", "3"])
ls_output3.communicate()