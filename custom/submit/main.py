# read in configuration
# populate .yaml file
# instantiate master

import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Group2 Custom Word Count Application. You should have a kubernetes configuration set up--check by execute `kubectl`')
    parser.add_argument('--csv', dest='csv', default='credentials.csv', help='CSV file with credentials')
    parser.add_argument('', dest='csv', default='credentials.csv', help='CSV file with credentials')

    args = parser.parse_args()

    username = None
    password = None
    key = None
    secret = None 

    with open(args.csv, 'r') as f:
        reader = csv.reader(f)
        info = [line for line in reader][1]
        username = info[0]
        password = info[1]
        key = info[2]
        secret = info[3]
