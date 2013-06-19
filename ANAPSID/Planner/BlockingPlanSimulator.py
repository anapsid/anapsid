'''
Created on Jul 10, 2011

Represents a blocking plan, where a process is created only
in the leaves of the tree (to contact the sources). The
intermediate results are represented as lists.

The source is the servers simulator.

@author: Maribel Acosta Deibe
'''
import string
from multiprocessing import Process, Queue
from MySPARQLWrapper.Wrapper import SPARQLWrapper, JSON
from ANAPSID.Catalog.Catalog import Catalog


def contactSource(server, query, queue):
    '''
    Contacts the datasource (i.e. endpoint).
    The hole answer is represented as a list that is stored in a queue.
    '''
    # Build the query and contact the source.
    sparql = SPARQLWrapper(server)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    res = sparql.query().convert()

    # Every tuple is appended in a list.
    reslist = res.split('\n')
    #resint = []
    for elem in reslist:
        #TODO: queue.put(eval(elem.rstrip()))
        queue.put(eval(elem.rstrip()))

    queue.put("EOF")
    # The list is added to the queue.
    #queue.put(resint)


class IndependentOperator(object):
    '''
    Implements an operator that can be resolved independently.

    It receives as input the url of the server to be contacted, and the
    filename that contains the query.

    The execute() method gets the list of results from a queue and returns it.
    '''
    def __init__(self, server, filename):
        self.server = server
        self.filename = filename
        self.q = None
        self.q = Queue()
        self.query = open(filename).read()
        self.p = Process(target=contactSource,
                         args=(self.server, self.query, self.q,))
        self.p.start()

    def execute(self, outputqueue):
        # Evaluate the independent operator.

        while True:
            # Get the next item in queue.
            res = self.q.get()
            # Put the result into the output queue.
            outputqueue.put(res)

            # Check if there's no more data.
            if (res == "EOF"):
                break
#        res = self.q.get()
#        #TODO:
#        #res = []
#        #elem = self.q.get()
#        #while (elem != "EOF"):
#        #    res.append(eval(elem.rstrip()))
#        #    elem = self.q.get()
#        self.p.join()
#
#        return res



class DependentOperator(object):
    '''
    Implements an operator that must be resolved with an instance.

    It receives as input the url of the server to be contacted,
    and the filename that contains the query.

    The execute() method performs a semantic check. If the instance
    can be derreferenced from the source, it will contact the source.
    '''
    def __init__(self, server, filename):
        self.server = server
        self.filename = filename
        self.q = None
        self.q = Queue()

        self.atts = self.getQueryAttributes()
        self.catalog = Catalog("../Catalog/endpoints.desc")

    def execute(self, variables, instances):
        res = []
        self.query = open(self.filename).read()

        # Replace in the query, the instance that is derreferenced.
        for i in range(len(variables)):
            self.query = string.replace(self.query, "?" + variables[i], "", 1)
            self.query = string.replace(self.query, "?" + variables[i], "<" + instances[i] + ">")

        # If the instance has no ?query. Example: DESCRIBE ---
        if (instances[0].find("sparql?query") == -1):
            pos = instances[0].find("/resource")
            pre = instances[0][0:pos]

            # Semantic check!.
            for server in self.server:
                prefixes = self.catalog.data[server]

                try:
                    # Contact the source.
                    pos = prefixes.index(pre)
                    self.p = Process(target=contactSource,
                              args=(server, self.query, self.q, self.sponge,))
                    self.p.start()
                    res = self.q.get()
                    self.p.join()
                except ValueError:
                    # The source shouldn't be contacted.
                    pass

        return res

    def getQueryAttributes(self):
        # Read the query from file and apply lower case.
        query = open(self.filename).read()
        query2 = string.lower(query)

        # Extract the variables, separated by commas.
        # TODO: it supposes that there's no from clause.
        begin = string.find(query2, "select")
        begin = begin + len("select")
        end = string.find(query2, "where")
        listatts = query[begin:end]
        listatts = string.split(listatts, " ")

        # Iterate over the list of attributes, and delete "?".
        outlist = []
        for att in listatts:
            if ((len(att) > 0) and (att[0] == '?')):
                if ((att[len(att)-1] == ',') or (att[len(att)-1] == '\n')):
                    outlist = outlist + [att[1:len(att)-1]]
                else:
                    outlist = outlist + [att[1:len(att)]]

        return outlist


class TreePlan(object):
    '''
    Represents a plan to be executed by the engine.

    It is composed by a left node, a right node, and an operator node.
    The left and right nodes can be leaves to contact sources, or subtrees.
    The operator node is a physical operator, provided by the engine.

    The execute() method evaluates the plan.
    The operator is evaluated when left and right are done.
    If the right node is an independent operator or a subtree, it is evaluated.
    '''
    def __init__(self, operator, left=None, right=None):
        self.operator = operator
        self.left = left
        self.right = right

    def execute(self, outputqueue):
        # Evaluates the execution plan.

        if self.left and self.right:
            qleft  = Queue()
            qright = Queue()
            # The left node is always evaluated.
            # Create process for left node
            p1 = Process(target=self.left.execute, args=(qleft,))
            p1.start()

            # Check the right node to determine if evaluate it or not.
            if ((self.right.__class__.__name__ == "IndependentOperator") or
                (self.right.__class__.__name__ == "TreePlan")):
                #qright = Queue()
                p2 = Process(target=self.right.execute, args=(qright,))
                p2.start()
            else:
                qright = self.right

        # Create a process for the operator node.
        self.p = Process(target=self.operator.execute,
                         args=(qleft, qright, outputqueue,))

        # Execute the plan
        self.p.start()

#        if self.left and self.right:
#            # The left node is always evaluated.
#            resleft = self.left.execute()
#
#            # Check the right node to determine if evaluate it or not.
#            if ((self.right.__class__.__name__ == "IndependentOperator") or
#                (self.right.__class__.__name__ == "TreePlan")):
#                resright = self.right.execute()
#            else:
#                resright = self.right
#
#        # Execute the operator.
#        return self.operator.execute(resleft, resright)
