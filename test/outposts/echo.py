#!/home/tony/Utilities/mambaforge/envs/lx/bin/python

import os, sys
os.system("touch /home/tony/workspace/python/Limes-all/Limes-x/test/outpost/x")

print("start")

with open(0, "rb") as ins:
    for i, x in enumerate(ins):
        if x == "exit": break
        print(f">{i}")
        sys.stdout.buffer.write(x)
        print(f"<{i}")


print("exit")
