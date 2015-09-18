from __future__ import print_function
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", 
                file=sys.stderr)
        exit(-1)

    for item in sys.argv:
        print(item)
