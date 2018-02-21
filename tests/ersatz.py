#!/usr/bin/env python

import argparse
import random
from time import sleep
import uuid

parser = argparse.ArgumentParser()
parser.add_argument('--inputs', nargs='*')
parser.add_argument('--arg1')
parser.add_argument('--arg2')
parser.add_argument('--outputs', nargs='*')
args = vars(parser.parse_args())
run_id = str(uuid.uuid4())[:8]

words = ('Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et '
         'dolore magna aliqua Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip '
         'ex ea commodo consequat Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu '
         'fugiat nulla pariatur Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt '
         'mollit anim id est laborum').split()

print('Input is {}'.format(args['inputs']))
sleep(random.randint(2, 12))
for output in args['outputs'] or list():
    with open(output, 'w') as out:
        for _ in range(random.randint(3, 20)):
            out.write(random.choice(words) + '\n')
print('Finished running ersatz {}'.format(run_id))
