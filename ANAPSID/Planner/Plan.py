'''
Created on Jul 10, 2011

Represents an adaptive plan, where a process is created in every
node of the execution tree. The intermediate results are represented
as Python dictionaries and are stored in queues.

@author: Maribel Acosta Deibe
@author: Gabriela Montoya

Last modification: December, 2013
'''
from __future__ import division
from multiprocessing import Process, Queue, active_children
from ANAPSID.Catalog.Catalog import Catalog
from ANAPSID.AnapsidOperators.Xgjoin import Xgjoin
from ANAPSID.AnapsidOperators.Xnjoin import Xnjoin
from ANAPSID.AnapsidOperators.Xgoptional import Xgoptional
from ANAPSID.AnapsidOperators.Xnoptional import Xnoptional
from ANAPSID.AnapsidOperators.Xunion import Xunion
from ANAPSID.AnapsidOperators.Xproject import Xproject
from ANAPSID.AnapsidOperators.Xdistinct import Xdistinct
from ANAPSID.NonBlockingOperators.SymmetricHashJoin import SymmetricHashJoin
from ANAPSID.NonBlockingOperators.NestedHashJoin import NestedHashJoin
from ANAPSID.NonBlockingOperators.NestedHashOptional import NestedHashOptional
from ANAPSID.BlockingOperators.HashJoin import HashJoin
from ANAPSID.BlockingOperators.HashOptional import HashOptional
from ANAPSID.BlockingOperators.NestedLoopOptional import NestedLoopOptional
from ANAPSID.BlockingOperators.NestedLoopJoin import NestedLoopJoin
from ANAPSID.BlockingOperators.Union import Union
from ANAPSID.Decomposer.Tree import Leaf, Node
from ANAPSID.Decomposer.services import Service, Argument, Triple, Filter, Optional
from ANAPSID.Decomposer.services import UnionBlock, JoinBlock, Query
from SPARQLWrapper import SPARQLWrapper, JSON, N3
import socket
import urllib
import string
import time
import signal
import sys, os

def contactSource(server, query, queue, buffersize=16384, limit=-1):
    
    #Contacts the datasource (i.e. real endpoint).
    #Every tuple in the answer is represented as Python dictionaries
    #and is stored in a queue.
    if (limit == -1):
        
        # Build the query and contact the source.
        sparql = SPARQLWrapper(server, queue)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        try:
            res = sparql.query()
        except Exception as e:
	    queue.put("EOF")
            return  None
        f = res.info()["content-type"]
        

        res = res.convert()
        b = None
	

        if type(res) == dict:
            b = res.get('boolean', None)
            
            if 'results' in res:
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


    #Contacts the datasource (i.e. real endpoint) incrementally, 
    #retreiving partial result sets combining the SPARQL sequence
    #modifiers LIMIT and OFFSET.
    #Every tuple in the answer is represented as Python dictionaries
    #and is stored in a queue.
    else:
         
        # Build the query and contact the source.
        sparql = SPARQLWrapper(server)
    
        # Set up to offset.
        offset = 0
        b = None
    
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            sparql.setQuery(query_copy)
            sparql.setReturnFormat(JSON)
        
            try:
                res = sparql.query()
            except Exception as e:
                queue.put("EOF")
                return  None
    
            f = res.info()["content-type"]
            res = res.convert()
            
        
            if type(res) == dict:
                b = res.get('boolean', None)
                if 'results' in res:
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
            #print "len(res[results][bindings]", len(res['results']['bindings']) 
            if (len(res['results']['bindings']) < limit):
                break
        
            offset = offset + limit
    
    #Close the queue
    if b == None:
        queue.put("EOF")
    return b

# Deprecated: Merged with contactSource (16-12-2013)
#def contactSourceIncrementally(server, query_original, queue, limit=50):
#    '''
#    Contacts the datasource (i.e. real endpoint)incrementally
#    retreiving partial result sets combining the SPARQL sequence
#    modifiers LIMIT and OFFSET.
#    Every tuple in the answer is represented as Python dictionaries
#    and is stored in a queue.
#    '''
#    # Build the query and contact the source.
#    sparql = SPARQLWrapper(server)
#    # Set up to offset.
#    offset = 0
#    b=None
#  
#    while True:
#      query = query_original + " LIMIT " + str(limit) + " OFFSET " + str(offset)
#      sparql.setQuery(query)
#      sparql.setReturnFormat(JSON)
#      
#      try:
#        res=sparql.query()
#      except:
#        queue.put("EOF")
#        return None
#      f = res.info()["content-type"]
#      res = res.convert()
#      if type(res) == dict:
#         b = res.get('boolean', None)
#         if 'results' in res:
#           for x in res['results']['bindings']:
#              for key, props in x.iteritems():
#                  x[key] = props['value']
#           reslist = res['results']['bindings']
#          # Every tuple is added to the queue.
#           for elem in reslist:
#             queue.put(elem)
#      else:
#        print ("the source "+str(server)+" answered in "+f+" format, instead of" +" the JSON format required, then that answer will be ignored")
#      if (len(res['results']['bindings']) < limit):
#         break
#      offset = offset + limit
#
#    #Close the queue
#    if b == None:
#        queue.put("EOF")
#    return b

# Deprecated: Merged with contactSourceIncrementally (16-12-2013)      
#def contactSource(server, query, queue, buffersize=16384):
#    '''
#    Contacts the datasource (i.e. real endpoint).
#    Every tuple in the answer is represented as Python dictionaries
#    and is stored in a queue.
#    '''
#
#    # Build the query and contact the source.
#    sparql = SPARQLWrapper(server, queue)
#    #print "query: "+str(query)
#    sparql.setQuery(query)
#    sparql.setReturnFormat(JSON)
#    try:
#        res = sparql.query()
#    except:
#        #print 'ignoring'
#        queue.put("EOF")
#        return  None
#    f = res.info()["content-type"]
#    res = res.convert()
#    b = None
#
#    if type(res) == dict:
#        b = res.get('boolean', None)
#	#print b
#        if 'results' in res:
#            for x in res['results']['bindings']:
#                for key, props in x.iteritems():
#                    x[key] = props['value']
#
#            reslist = res['results']['bindings']
#
#            # Every tuple is added to the queue.
#            for elem in reslist:
#                #print elem
#                queue.put(elem)
#    else:
#        
#        print res
#        print ("the source "+str(server)+" answered in "+f+" format, instead of"
#               +" the JSON format required, then that answer will be ignored")
#    #Close the queue
#    if b == None:
#        #print 'aqui'
#        queue.put("EOF")
#    return b

def contactProxy(server, query, queue, buffersize=16384, limit=50):
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
    b = None
    lb = True
    #Receive the rest of the messages.
    while True:
      try:
        data = s.recv(buffersize)
      except Exception:
        exit()
      else:
        #print "data_contactProxy: "+str(data)
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
        if lb and (len(reslist) > 0):
            l = reslist[0]
            p = l.find(', \"boolean\": ')
            if p >= 0 and len(l) > p + 13:
                #print "contactProxy_l: "+str(l)
		b = (l[p+13] == 't')
                lb = False
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
                lb = False
            else:
                aux = elem
        if tam > -1 and ac >= tam:
            break
    if b == None:
        queue.put("EOF")
    #Close the connection
    s.close()
    
    return b

def createPlan(query, adaptive, wc, buffersize, c):
    #print "create Plan: Plan.py 167"
    operatorTree = includePhysicalOperatorsQuery(query, adaptive, wc,
                                                 buffersize, c)
    
    # Adds the project operator to the plan.
    if (query.args != []):
        operatorTree = TreePlan(Xproject(query.args), operatorTree.vars, operatorTree)

    # Adds the distinct operator to the plan.
    if (query.distinct):
        operatorTree = TreePlan(Xdistinct(None), operatorTree.vars, operatorTree)

    #print "opertator type:", operatorTree
    return operatorTree

def includePhysicalOperatorsQuery(query, a, wc, buffersize, c):
    return includePhysicalOperatorsUnionBlock(query, query.body,
                                              a, wc, buffersize, c)

def includePhysicalOperatorsUnionBlock(query, ub, a, wc, buffersize, c):

    r = []
    for jb in ub.triples:
        r.append(includePhysicalOperatorsJoinBlock(query, jb,
                                                   a, wc, buffersize, c))
    while len(r) > 1:
        left = r.pop(0)
        right = r.pop(0)

        all_variables  = left.vars | right.vars

        if a:
            n =  TreePlan(Xunion(left.vars, right.vars),
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
            lowSelectivityLeft = l.allTriplesLowSelectivity()
            lowSelectivityRight = right.allTriplesLowSelectivity()
            join_variables = l.vars & right.vars

            dependent_op = False
            # Case 1: left operator is highly selective and right operator is low selective
            if not(lowSelectivityLeft) and lowSelectivityRight:
                l = TreePlan(NestedHashOptional(left.vars, right.vars), all_variables, l, right)
                dependent_op = True
                #print "Planner CASE 1: nested optional"

            # Case 2: left operator is low selective and right operator is highly selective
            elif lowSelectivityLeft and not(lowSelectivityRight):
                l = TreePlan(NestedHashOptional(left.vars, right.vars), all_variables, right, l)
                dependent_op = True
                #print "Planner CASE 2: nested loop optional swapping plan"

            # Case 3: both operators are low selective
            else:
                 l =  TreePlan(Xgoptional(left.vars, right.vars), all_variables, l, right)
                 #print "Planner CASE 3: xgoptional"

            
            if isinstance(l.left, IndependentOperator) and isinstance(l.left.tree, Leaf):
                if (l.left.constantPercentage() <= 0.5):
                    l.left.tree.service.limit = 10000 # Fixed value, this can be learnt in the future 
                    #print "modifying limit optional left ..."

            if isinstance(l.right, IndependentOperator) and isinstance(l.right.tree, Leaf):
                if not(dependent_op):
                    if (l.right.constantPercentage() <= 0.5):
                        l.right.tree.service.limit = 10000 # Fixed value, this can be learnt in the future 
                        #print "modifying limit optional right ..."
                else:
                    new_constants = 0
                    for v in join_variables:
                        new_constants = new_constants + l.right.query.show().count(v)
                    if ((l.right.constantNumber() + new_constants)/l.right.places() <= 0.5):
                        l.right.tree.service.limit = 10000 # Fixed value, this can be learnt in the future
                        #print "modifying limit optional right ..."

        else:
            l = TreePlan(HashOptional(left.vars, right.vars),
                         all_variables, l, right)
    return l

def includePhysicalOperatorsJoinBlock(query, jb, a, wc, buffersize, c):

    tl = []
    ol = []
    if isinstance(jb.triples, list):
        for bgp in jb.triples:
            if isinstance(bgp, Node) or isinstance(bgp, Leaf):
                tl.append(includePhysicalOperators(query, bgp, a, wc,
                                                   buffersize, c))
            elif isinstance(bgp, Optional):
                ol.append(includePhysicalOperatorsUnionBlock(query,
                          bgp.bgg, a, wc, buffersize, c))
            elif isinstance(bgp, UnionBlock):
                tl.append(includePhysicalOperatorsUnionBlock(query,
                                                             bgp, a, wc, buffersize, c))
    elif isinstance(jb.triples, Node) or isinstance(jb.triples, Leaf):
        tl = [includePhysicalOperators(query, jb.triples, a, wc, buffersize, c)]
    else: # this should never be the case..
        pass
        #print "type of triples: "+str(type(jb.triples))

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
    noInstantiatedLeftStar = False
    noInstantiatedRightStar = False
    lowSelectivityLeft = l.allTriplesLowSelectivity()
    lowSelectivityRight = r.allTriplesLowSelectivity()

    #print wc
    #print join_variables
    #print l.allTriplesLowSelectivity()
    if a:
        #if lowSelectivityLeft or (len(join_variables) == 0):
        #    c = False
        #elif wc:
        #    c = True
        #else:
        #    lsc = l.getCardinality()
        #    c = (lsc <= 30)
        #    if c and not lowSelectivityRight:
        #        c = c and (lsc <= 0.3*r.getCardinality())
        
        dependent_join = False
        #if (noInstantiatedRightStar) or ((not wc) and (l.constantPercentage() >= 0.5) and (len(join_variables) > 0) and c):
        # Case 1: left operator is highly selective and right operator is low selective
	if not(lowSelectivityLeft) and lowSelectivityRight:
            n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
            dependent_join = True
            #print "Planner CASE 1: nested loop"
	
        # Case 2: left operator is low selective and right operator is highly selective
	elif lowSelectivityLeft and not(lowSelectivityRight):
	    n = TreePlan(NestedHashJoin(join_variables), all_variables, r, l)
            dependent_join = True
            #print "Planner CASE 2: nested loop swapping plan"	
        # Case 3: both operators are low selective
	else:
	    n =  TreePlan(Xgjoin(join_variables), all_variables, l, r)
            #print "Planner CASE 3: xgjoin"

	if isinstance(n.left, IndependentOperator) and isinstance(n.left.tree, Leaf):
	    if (n.left.constantPercentage() <= 0.5):
                n.left.tree.service.limit = 10000 # Fixed value, this can be learnt in the future 
                #print "modifying limit left ..."   

        if isinstance(n.right, IndependentOperator) and isinstance(n.right.tree, Leaf):
            if not(dependent_join):
                if (n.right.constantPercentage() <= 0.5):
                    n.right.tree.service.limit = 10000 # Fixed value, this can be learnt in the future
                    #print "modifying limit right ..."
            else:
                new_constants = 0
                for v in join_variables:
                    new_constants = new_constants + n.right.query.show().count(v)
                if ((n.right.constantNumber() + new_constants)/n.right.places() <= 0.5):
                    n.right.tree.service.limit = 10000 # Fixed value, this can be learnt in the future
                    #print "modifying limit right ..."
            
     
    else:
        n =  TreePlan(HashJoin(join_variables), all_variables, l, r)
    return n


#def includePhysicalOperatorJoin(a, wc, l, r):
#    join_variables = l.vars & r.vars
#    all_variables  = l.vars | r.vars
#    noInstantiatedStar = False
#    print wc
#    print join_variables
#    print l.allTriplesLowSelectivity()
#    if a:
#        if l.allTriplesLowSelectivity() or (len(join_variables) == 0):
#            c = False
#        elif wc:
#            c = True
#        else:
#            lsc = l.getCardinality()
#            c = (lsc <= 30)
#            if c and not r.allTriplesLowSelectivity():
#                c = c and (lsc <= 0.3*r.getCardinality())
#        if (l.constantPercentage() == float(1.0)/3) or (r.constantPercentage() == float(1.0)/3):
#              noInstantiatedStar = True
#        if (noInstantiatedStar) or ((not wc) and (l.constantPercentage() >= 0.5) and (len(join_variables) > 0) and c):
#            n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
#        else:
#            #n = TreePlan(NestedHashJoin(join_variables), all_variables, l, r)
#            n =  TreePlan(Xgjoin(join_variables), all_variables, l, r)
#    else:
#        n =  TreePlan(HashJoin(join_variables), all_variables, l, r)
#    return n

def includePhysicalOperators(query, tree, a, wc, buffersize, c):
    
    if isinstance(tree, Leaf):
        if isinstance(tree.service, Service):
            return IndependentOperator(query, tree, c, buffersize)
        elif isinstance(tree.service, UnionBlock):
            return includePhysicalOperatorsUnionBlock(query, tree.service,
                                                      a, wc, buffersize, c)
        else:
            print "Plan.py:258"

    elif isinstance(tree, Node):

        left_subtree = includePhysicalOperators(query, tree.left,
                                                a, wc, buffersize, c)
        right_subtree = includePhysicalOperators(query, tree.right,
                                                 a, wc, buffersize, c)
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
    def __init__(self, query, tree, c, buffersize=16384):

        (e, sq, vs) = tree.getInfoIO(query)
        self.contact = c
        self.server = e
        self.query = query
        self.tree = tree
        self.query_str = sq
        self.vars = vs
        self.buffersize = buffersize
        self.cardinality = None
        self.joinCardinality = []
	#self.limit = limit

    def instantiate(self, d):
        new_tree = self.tree.instantiate(d)
        return IndependentOperator(self.query, new_tree, self.contact,
                                   self.buffersize)

    def getCardinality(self):
        if self.cardinality == None:
            self.cardinality = askCount(self.query, self.tree, set(), self.contact)
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
                c = askCount(self.query, self.tree, vars, self.contact)
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
        self.p = Process(target=self.contact,
                         args=(self.server, self.query_str,
                               self.q, self.buffersize, self.tree.service.limit,)) 
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

def askCount(query, tree, vars, contact):

    (server, query) = tree.getCount(query, vars)
#    print "queryCount" + query
    q = Queue()
    b = contact(server, query, q)

    
    res = q.get()
    #print res
    if (res == "EOF"):
        return 20000
    for k in res:
        v = res[k]
    q.get()
    return int(v)

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
        if self.left: #and this.right: # This line was modified by mac in order to evaluate unary operators
            qleft  = Queue()
            qright = Queue()

            # The left node is always evaluated.
            # Create process for left node
            p1 = Process(target=self.left.execute, args=(qleft,))
            p1.start()

            if ((self.operator.__class__.__name__ == "NestedHashJoin") or
                (self.operator.__class__.__name__ == "NestedHashOptional")):
                self.p = Process(target=self.operator.execute,
                                 args=(qleft, self.right, outputqueue,))
                self.p.start()
                return

            # Check the right node to determine if evaluate it or not.
            if (self.right and ((self.right.__class__.__name__ == "IndependentOperator") or
                (self.right.__class__.__name__ == "TreePlan"))):
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
