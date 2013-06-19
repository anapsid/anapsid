'''
Created on Jul 10, 2011

Represents an adaptive plan, where a process is created in every
node of the execution tree. The intermediate results are represented
as Python dictionaries and are stored in queues.

@author: Maribel Acosta Deibe
@author: Gabriela Montoya

Last modification: August, 2012
'''
from __future__ import division
from multiprocessing import Process, Queue, active_children
import socket
import urllib
import string
import time
import signal
import sys, os
from SPARQLWrapper import SPARQLWrapper, JSON
from ANAPSID.Catalog.Catalog import Catalog
from ANAPSID.AnapsidOperators.Xgjoin import Xgjoin
from ANAPSID.AnapsidOperators.Xnjoin import Xnjoin
from ANAPSID.AnapsidOperators.Xgoptional import Xgoptional
from ANAPSID.AnapsidOperators.Xunion import Xunion
from ANAPSID.AnapsidOperators.Xproject import Xproject
from ANAPSID.NonBlockingOperators.SymmetricHashJoin import SymmetricHashJoin
from ANAPSID.NonBlockingOperators.NestedHashJoin import NestedHashJoin
from ANAPSID.BlockingOperators.HashJoin import HashJoin
from ANAPSID.BlockingOperators.HashOptional import HashOptional
from ANAPSID.BlockingOperators.NestedLoopOptional import NestedLoopOptional
from ANAPSID.BlockingOperators.NestedLoopJoin import NestedLoopJoin
from ANAPSID.BlockingOperators.Union import Union
from ANAPSID.Decomposer.Tree import Leaf, Node
from ANAPSID.Decomposer.services import Service, Argument, Triple, Filter, Optional
from ANAPSID.Decomposer.services import UnionBlock, JoinBlock, Query

def contactSource(server, query, queue):
    '''
    Contacts the datasource (i.e. real endpoint).
    Every tuple in the answer is represented as Python dictionaries
    and is stored in a queue.
    '''

    # Build the query and contact the source.
    sparql = SPARQLWrapper(server)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    res = sparql.query()
    f = res.info()["content-type"]
    res = res.convert()

    if type(res) == dict:
        for x in res['results']['bindings']:
            for key, props in x.iteritems():
                x[key] = props['value']

        reslist = res['results']['bindings']

        # Every tuple is added to the queue.
        for elem in reslist:
            queue.put(elem)
    else:
        print ("the source "+str(server)+" answered in "+f+" format, instead of"
               +" the JSON format required, then that answer will be ignored")
    #Close the queue
    queue.put("EOF")

def contactProxy(server, query, buffersize, queue):
    '''
    Contacts the proxy (i.e. simulator that can divede the answer in packages)
    Every tuple in the answer is represented as Python dictionaries
    and is stored in a queue.
    '''
    # Encode the query as an url string.
    query = urllib.quote(query.encode('utf-8'))
    format = urllib.quote("application/sparql-results+json".encode('utf-8'))
    #Get host and port from "server".
    [http, server] = server.split("http://")
    host_port = server.split(":")

    port= host_port[1].split("/")[0]

    # Create socket, connect it to server and send the query.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.connect((host_port[0], int(port)))

    s.send("GET sparql/?query=" + query + "&format=" + format)
    s.shutdown(1)

    aux = ""
    headerStr = ''
    tam = -1
    ac = -1
    aux2 = ""
    #Receive the rest of the messages.
    while True:
        data = s.recv(buffersize)
        print "data_contactProxy: "+str(data)
        if len(data) == 0:
            continue
        if tam == -1:
            headerStr = headerStr + data
            pos = headerStr.find('Content-Length: ')
            if pos > -1:
                rest = headerStr[(pos+16):]
                pos2 = rest.find('\n')
                if pos2 > -1:
                    tam = int(rest[:pos2])
        if ac == -1:
            aux2 = aux2 + data
            pos = (aux2).find('\n\r\n')
            if pos > -1:
                ac = len(aux2) - pos - 3
        else:
            ac = ac + len(data)

        data = aux + data
        reslist = data.split('\n')

        for elem in reslist:
            pos1 = string.find(elem, "    {")
            pos2 = string.find(elem, "}}")
            if ((pos1>-1) and (pos2>-1)):
                str_t = elem[pos1:pos2+2]
                dict_t = eval(str_t.rstrip())
                res = {}
                for key, props in dict_t.iteritems():
                    res[key] = props['value']
                queue.put(res)
                aux = elem[pos2:]
            else:
                aux = elem
        if tam > -1 and ac >= tam:
            break

    queue.put("EOF")
    #Close the connection
    s.close()

def createPlan(simulated, query, adaptive, withoutCounts, buffersize):

    operatorTree = includePhysicalOperatorsQuery(simulated, query, adaptive,
                                                 withoutCounts, buffersize)
    #print(operatorTree.aux(""))
    return operatorTree

def includePhysicalOperatorsQuery(simulated, query, a, wc, buffersize):
    return includePhysicalOperatorsUnionBlock(simulated, query, query.body,
                                              a, wc, buffersize)

def includePhysicalOperatorsUnionBlock(simulated, query, ub, a, wc, buffersize):

    r = []
    for jb in ub.triples:
        r.append(includePhysicalOperatorsJoinBlock(simulated, query, jb,
                                                   a, wc, buffersize))

    while len(r) > 1:
        left = r.pop(0)
        right = r.pop(0)

        all_variables  = left.vars | right.vars

        if a:
            n =  TreePlan(Xunion(left.vars, right.vars, query.distinct),
                          all_variables, left, right)
        else:
            n =  TreePlan(Union(left.vars, right.vars, query.distinct),
                          all_variables, left, right)
        r.append(n)

    if len(r) == 1:
        return r[0]
    else:
        return None

def includePhysicalOperatorsOptional(left, rightList, a):

    l = left

    for right in rightList:
        all_variables  = left.vars | right.vars
        if a:
            l = TreePlan(Xgoptional(left.vars, right.vars),
                         all_variables, l, right)
        else:
            l = TreePlan(HashOptional(left.vars, right.vars),
                         all_variables, l, right)
    return l

def includePhysicalOperatorsJoinBlock(simulated, query, jb, a, wc, buffersize):

    tl = []
    ol = []

    if isinstance(jb.triples, list):
        for bgp in jb.triples:
            if isinstance(bgp, Node) or isinstance(bgp, Leaf):
                tl.append(includePhysicalOperators(simulated, query, bgp, a,
                                                   wc, buffersize))
            elif isinstance(bgp, Optional):
                ol.append(includePhysicalOperatorsUnionBlock(simulated, query,
                          bgp.bgg, a, wc, buffersize))
            elif isinstance(bgp, UnionBlock):
                tl.append(includePhysicalOperatorsUnionBlock(simulated, query,
                                                             bgp, a, wc, buffersize))
    elif isinstance(jb.triples, Node) or isinstance(jb.triples, Leaf):
        tl = [includePhysicalOperators(simulated, query, jb.triples, a, wc, buffersize)]
    else: # this should never be the case..
        print "type of triples: "+str(type(jb.triples))

    while len(tl) > 1:
        l = tl.pop(0)
        r = tl.pop(0)

        n = includePhysicalOperatorJoin(a, wc, l, r)

        tl.append(n)

    if len(tl) == 1:
        return includePhysicalOperatorsOptional(tl[0], ol, a)
    else:
        return None

def includePhysicalOperatorJoin(a, wc, l, r):
    join_variables = l.vars & r.vars
    all_variables  = l.vars | r.vars

    if a:
        if l.allTriplesLowSelectivity() or (len(join_variables) == 0):
            c = False
        elif wc:
            c = True
        else:
            lsc = l.getCardinality()
            c = (lsc <= 30)
            if c and not r.allTriplesLowSelectivity():
                c = c and (lsc <= 0.3*r.getCardinality())

        if (not wc) and (l.constantPercentage() >= 0.5) and (len(join_variables) > 0) and c:
            n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
        else:
            n =  TreePlan(Xgjoin(join_variables), all_variables, l, r)
    else:
        n =  TreePlan(HashJoin(join_variables), all_variables, l, r)
    return n

def includePhysicalOperators(simulated, query, tree, a, wc, buffersize):

    if isinstance(tree, Leaf):
        if isinstance(tree.service, Service):
            return IndependentOperator(simulated, query, tree, buffersize)
        elif isinstance(tree.service, UnionBlock):
            return includePhysicalOperatorsUnionBlock(simulated, query,
                                                      tree.service, a, wc, buffersize)
        else:
            print "Plan.py:258"

    elif isinstance(tree, Node):

        left_subtree = includePhysicalOperators(simulated, query, tree.left,
                                                a, wc, buffersize)
        right_subtree = includePhysicalOperators(simulated, query, tree.right,
                                                 a, wc, buffersize)
        return includePhysicalOperatorJoin(a, wc, left_subtree, right_subtree)

class IndependentOperator(object):
    '''
    Implements an operator that can be resolved independently.

    It receives as input the url of the server to be contacted,
    the filename that contains the query, the header size of the
    of the messages.

    The execute() method reads tuples from the input queue and
    response message and the buffer size (length of the string)
    place them in the output queue.
    '''
    def __init__(self, simulated, query, tree, buffersize=100):

        (e, sq, vs) = tree.getInfoIO(query)
        self.simulated = simulated
        self.server = e
        self.query = query
        self.tree = tree
        self.query_str = sq
        self.vars = vs
        self.buffersize = buffersize
        self.cardinality = None
        self.joinCardinality = []

    def instantiate(self, d):
        new_tree = self.tree.instantiate(d)
        return IndependentOperator(self.query, new_tree, self.buffersize)

    def getCardinality(self):
        if self.cardinality == None:
            self.cardinality = askCount(self.query, self.tree, set())
        return self.cardinality

    def getJoinCardinality(self, vars):
        c = None
        for (v, c2) in self.joinCardinality:
            if v == vars:
                c = c2
                break
        if c == None:
            if len(vars) == 0:
                c = self.getCardinality()
            else:
                c = askCount(self.query, self.tree, vars)
            self.joinCardinality.append((vars, c))
        return c

    def allTriplesLowSelectivity(self):
        return self.tree.service.allTriplesLowSelectivity()

    def places(self):
        return self.tree.places()

    def constantNumber(self):
        return self.tree.constantNumber()

    def constantPercentage(self):
        return self.constantNumber()/self.places()

    def aux(self, n):
        return self.tree.aux(n)

    def execute(self, outputqueue):
        # Evaluate the independent operator.
        self.q = None
        self.q = Queue()
        if self.simulated:
            self.p = Process(target=contactProxy,
                             args=(self.server, self.query_str,
                                   self.buffersize, self.q,))
        else:
            self.p = Process(target=contactSource,
                             args=(self.server, self.query_str, self.q,))
        self.p.start()

        while True:
            # Get the next item in queue.
            res = self.q.get(True)
            # Put the result into the output queue.
            #print res
            outputqueue.put(res)

            # Check if there's no more data.
            if (res == "EOF"):
                break

    def __repr__(self):
        return str(self.tree)

def askCount(query, tree, vars):

    (server, query) = tree.getCount(query, vars)
    query = urllib.quote(query.encode('utf-8'))
    format = urllib.quote("application/sparql-results+json".encode('utf-8'))
    [http, server] = server.split("http://")
    host_port = server.split(":")

    port= host_port[1].split("/")[0]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host_port[0], int(port)))
    s.send("GET sparql/?query=" + query + "&format=" + format)
    s.shutdown(1)

    aux = ""
    headerStr = ''
    tam = -1
    ac = -1
    buffersize = 1500
    sigue = True
    res = {}
    #Receive the rest of the messages.
    while sigue:
        data = s.recv(buffersize)
        if tam == -1:
            headerStr = headerStr + data
            pos = headerStr.find('Content-Length: ')
            if pos > -1:
                rest = headerStr[(pos+16):]
                pos2 = rest.find('\n')
                if pos2 > -1:
                    tam = int(rest[:pos2])

        aux = aux + data
        if ac == -1:
            pos = aux.find('\n\r\n')
            if pos > -1:
                ac = len(aux) - pos - 3
        else:
            ac = ac + len(data)

        data = aux
        reslist = data.split('\n')
        for elem in reslist:
            pos1 = string.find(elem, "    {")
            pos2 = string.find(elem, "}}")
            if ((pos1>-1) and (pos2>-1)):
                str_t = elem[pos1:pos2+2]
                dict_t = eval(str_t.rstrip())
                res = {}
                for key, props in dict_t.iteritems():
                    res[key] = props['value']
                sigue = False
                aux = elem[pos2:]
            else:
                aux = elem

        if tam > -1 and ac >= tam:
            break
    r = 0
    for k in res:
        r = res[k]

    s.close()
    return int(r)

def onSignal(s, stackframe):

    cs = active_children()
    for c in cs:
      try:
        os.kill(c.pid, s)
      except OSError as ex:
        continue
    sys.exit(s)

class DependentOperator(object):
    '''
    Implements an operator that must be resolved with an instance.

    It receives as input the url of the server to be contacted,
    the filename that contains the query, the header size of the
    response message, the buffer size (length of the string) of the
    messages.

    The execute() method performs a semantic check. If the instance
    can be derreferenced from the source, it will contact the source.
    '''

    def __init__(self, server, query, vs, buffersize): #headersize ???
        self.server = server
        #self.filename = filename
        self.query = query
        #self.headersize = headersize
        self.buffersize = buffersize
        self.q = None
        self.q = Queue()
        self.atts = vs
        self.prefs = [] #query.prefs
        #self.atts = self.getQueryAttributes()
        #self.catalog = Catalog("/home/gabriela/Anapsid/src/Catalog/endpoints.desc")


    def execute(self, variables, instances, outputqueue):

        self.query = open(self.filename).read()
        # ? signal.signal(12, onSignal)
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
                              args=(server, self.query, self.headersize, self.buffersize, self.q,))
                    self.p.start()

#                    first_tuple = True

                    while True:
                        # Get the next item in queue.
                        res = self.q.get()

#                        #Get the variables from the answer
#                        if (first_tuple):
#                            vars = res.keys()
#                            outputqueue.put(vars)
#                            first_tuple = False

                        # Put the result into the output queue.
                        outputqueue.put(res)

                        # Check if there's no more data.
                        if (res == "EOF"):
                            break

                except ValueError:
                    # The source shouldn't be contacted.
                    outputqueue.put(self.atts)
                    outputqueue.put("EOF")


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
    It creates a process for every node of the plan.
    The left node is always evaluated.
    If the right node is an independent operator or a subtree, it is evaluated.
    '''
    def __init__(self, operator, vars, left=None, right=None):
        self.operator = operator
        self.vars = vars
        self.left = left
        self.right = right
        self.cardinality = None
        self.joinCardinality = []

    def __repr__(self):
        return self.aux(" ")

    def instantiate(self, d):
        l = None
        r = None
        if self.left:
            l = self.left.instantiate(d)
        if self.right:
            r = self.right.instantiate(d)
        newvars = self.vars - set(d.keys())
        return TreePlan(self.operator.instantiate(d), newvars, l, r)

    def allTriplesLowSelectivity(self):
        a = True
        if self.left:
            a = self.left.allTriplesLowSelectivity()
        if self.right:
            a = a and self.right.allTriplesLowSelectivity()
        return a

    def places(self):
        p = 0
        if self.left:
            p = self.left.places()
        if self.right:
            p = p + self.right.places()
        return p

    def constantNumber(self):
        c = 0
        if self.left:
            c = self.left.constantNumber()
        if self.right:
            c = c + self.right.constantNumber()
        return c

    def constantPercentage(self):
        return self.constantNumber()/self.places()

    def getCardinality(self):

        if self.cardinality == None:
            self.cardinality = self.operator.getCardinality(self.left, self.right)
        return self.cardinality

    def getJoinCardinality(self, vars):
        c = None
        for (v, c2) in self.joinCardinality:
            if v == vars:
                c = c2
                break
        if c == None:
            c = self.operator.getJoinCardinality(self.left, self.right, vars)
            self.joinCardinality.append((vars, c))
        return c

    def aux(self, n):
        s = n + str(self.operator) + "\n" + n + str(self.vars) + "\n"
        if self.left:
            s = s + self.left.aux(n+"  ")

        if self.right:
            s = s + self.right.aux(n+"  ")
        return s

    def execute(self, outputqueue):
        # Evaluates the execution plan.
        if self.left and self.right:
            qleft  = Queue()
            qright = Queue()

            # The left node is always evaluated.
            # Create process for left node
            p1 = Process(target=self.left.execute, args=(qleft,))
            p1.start()

            if (self.operator.__class__.__name__ == "NestedHashJoin"):
                self.p = Process(target=self.operator.execute,
                                 args=(qleft, self.right, outputqueue,))
                self.p.start()
                return

            # Check the right node to determine if evaluate it or not.
            if ((self.right.__class__.__name__ == "IndependentOperator") or
                (self.right.__class__.__name__ == "TreePlan")):
                qright = Queue()
                p2 = Process(target=self.right.execute, args=(qright,))
                p2.start()
            else:
                qright = self.right

            # Create a process for the operator node.
            self.p = Process(target=self.operator.execute,
                             args=(qleft, qright, outputqueue,))
            # Execute the plan
            self.p.start()
