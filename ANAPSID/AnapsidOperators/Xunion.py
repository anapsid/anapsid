'''
Created on Jul 10, 2011

Implements the Xunion operator.
The intermediate results are represented in a queue.

@author: Maribel Acosta Deibe
'''
from multiprocessing import Queue
#from collections import Counter
from ANAPSID.Operators.Union import _Union

class Xunion(_Union):

    def __init__(self, vars_left, vars_right, distinct):
        self.left       = Queue()
        self.right      = Queue()
        self.bag        = [] #Counter()
        self.qresults   = Queue()
        self.vars_left  = vars_left
        self.vars_right = vars_right
        self.distinct   = distinct

    def instantiate(self, d):
        newvars_left = self.vars_left - set(d.keys())
        newvars_right = self.vars_right - set(d.keys())
        return Xunion(newvars_left, newvars_right, self.distinct)

    def execute(self, left, right, out):
        # Executes the Xunion.
        self.left = left
        self.right = right
        self.qresults = out

        # Identify the kind of union to perform.
        if (self.vars_left == self.vars_right):
            self.sameVariables()
        else:
            self.differentVariables()
        #print "cardinalidad: "+str(len(self.qresults))
        # Put EOF in queue and exit.
        self.qresults.put("EOF")
        return

    def sameVariables(self):
        # Executes the Xunion operator when the variables are the same.

        # Initialize tuples.
        tuple1 = None
        tuple2 = None

        # Get the tuples from the queues.
        while (not(tuple1 == "EOF") or not(tuple2 == "EOF")):
            if (not(tuple1 == "EOF")):
                try:
                    tuple1 = self.left.get(False)
                    if (not (tuple1 == "EOF")) and (not(self.distinct) or (not (tuple1 in self.bag))):
                        self.qresults.put(tuple1)
                        self.bag.append(tuple1)
                        #print(tuple1)

                except Exception:
                    # This catch:
                    # Empty: in tuple1 = self.left.get(False), when the queue is empty.
                    pass

            if (not(tuple2 == "EOF")):
                try:
                    tuple2 = self.right.get(False)
                    #print("tuple2: "+str(tuple2))
                    #print("len-bag: "+str(len(self.bag)))
                    #print("bag: "+str(self.bag))
                    #print("c: "+str(tuple2 in self.bag))
                    #print("type t: "+str(type(tuple2)))
                    if (not (tuple2 == "EOF")) and (not(self.distinct) or (not(tuple2 in self.bag))):
                        self.qresults.put(tuple2)
                        self.bag.append(tuple2)
                        #print(tuple2)
                except Exception:
                    # This catch:
                    # Empty: in tuple2 = self.right.get(False), when the queue is empty.
                    pass


    def differentVariables(self):
        # Executes the Xunion operator when the variables are not the same.

        # Initialize tuples.
        tuple1 = None
        tuple2 = None

        # Get the tuples from the queues.
        while (not(tuple1 == "EOF") or not(tuple2 == "EOF")):

            # Get tuple from left queue, and concatenate with empty tuple.
            if (not(tuple1 == "EOF")):
                try:
                    tuple1 = self.left.get(False)
                    #print("tuple1: "+str(tuple1))
                    #print("len-bag: "+str(len(self.bag)))
                    #print("bag: "+str(self.bag))
                    #print("c: "+str(tuple1 in self.bag))
                    #print("type t: "+str(type(tuple1)))
                    if not(self.distinct) or (not(tuple1 in self.bag)):
                        res = {}
                        for v in self.vars_right:
                            res.update({v:''})
                        res.update(tuple1)
                        self.qresults.put(res)
                        self.bag.append(tuple1)
                        #print(tuple1)
                except Exception:
                    # This catch:
                    # Empty: in tuple1 = self.left.get(False), when the queue is empty.
                    pass

            # Get tuple from right queue, and concatenate with empty tuple.
            if (not(tuple2 == "EOF")):
                try:
                    tuple2 = self.right.get(False)
                    #print("tuple2: "+str(tuple2))
                    #print("len-bag: "+str(len(self.bag)))
                    #print("bag: "+str(self.bag))
                    #print("c: "+str(tuple2 in self.bag))
                    #print("type t: "+str(type(tuple2)))
                    if not(self.distinct) or (not(tuple2 in self.bag)):
                        res = {}
                        for v in self.vars_left:
                            res.update({v:''})
                        res.update(tuple2)
                        self.qresults.put(res)
                        self.bag.append(tuple2)
                        #print(tuple2)
                except Exception:
                    # This catch:
                    # Empty: in tuple2 = self.right.get(False), when the queue is empty.
                    pass
