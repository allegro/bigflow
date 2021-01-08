import sys
import bigflow.cli

if __name__ == '__main__':
    sys.argv[0] = "bigflow"
    bigflow.cli.cli(sys.argv[1:])
