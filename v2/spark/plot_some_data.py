import os
import re
from datetime import datetime
from datetime import timedelta
import random
import matplotlib.pyplot as plt
from collections import Counter


line_regex = r'^\s+0-39\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+([0-9]+[\.]?[0-9]+)\s+[^\s]+$'


def main():
    file_path = "D:\Power Measurements\\v2\Exp-2019-02-19-10-02-33\mb_output"
    values = Counter()
    i = 0
    with open(file_path, "r") as lines:
        for line in lines:
            matches = re.match(line_regex, line)
            if matches:
                value = float(matches.group(1))
                values[i] = value
                i += 1
    
    fig, ax = plt.subplots(1, 1)
    fig.suptitle("Local Mem Bandwidth ")
    
    ax.set_xlabel("Seconds")
    ax.set_ylabel("MB/s")
    ax.plot(values.keys(), values.values())
    plt.show()


if __name__ == '__main__':
    main()