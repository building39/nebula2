#!/usr/bin/env python
'''
Created on Jun 8, 2013

@author: mmartin
'''

import os
import  sys
import CDMIMain as cdmi


def which(filen):
    for path in sys.path:
        if os.path.exists(path + "/" + filen):
                return path
    return None

if __name__ == '__main__':

    ppath = which(os.path.basename(sys.argv[0]))

    cdmi.CDMIMainWindow(ppath)
