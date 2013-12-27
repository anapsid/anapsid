'''
Created on Dec 11, 2013

Implements the Xdistinct operator.
The intermediate results are represented in a queue. 

@author: Maribel Acosta Deibe
'''
from multiprocessing import Queue

class Xdistinct(object):
    
    def __init__(self, vars):
        #self.input       = Queue()
        self.qresults   = Queue()
        self.vars  = vars
        self.bag = []
        
    def execute(self, left, dummy, out):
        # Executes the Xdistinct.
        self.left = left
        self.qresults = out
        tuple = self.left.get(True)
        
        while (not(tuple == "EOF")):
            if not(tuple in self.bag):
                self.qresults.put(tuple)
                self.bag.append(tuple)
            tuple = self.left.get(True)
            
        # Put EOF in queue and exit. 
        self.qresults.put("EOF")
        return
    
