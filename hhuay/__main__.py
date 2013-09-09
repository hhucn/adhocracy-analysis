#!/usr/bin/env python3

# Execute with  python3 -m hhuay

import sys

if __package__ is None and not hasattr(sys, "frozen"):
    # direct call of __main__.py
    import os.path
    path = os.path.realpath(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(os.path.dirname(path)))

import hhuay

if __name__ == '__main__':
    hhuay.main()
