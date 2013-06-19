from __future__ import division
import math
import heapq
import string
import abc

class Tree(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def instantiate(self, d):
        return

    def degree(self):
        return getDegree(self.vars, self.dict)

    def __leq__ (self, other):
        return (self.size < other.size or (self.size == other.size 
                                           and self.degree() <= other.degree()))

    def __lt__ (self, other):
        return (self.size < other.size or (self.size == other.size 
                                           and self.degree() < other.degree()))

    @abc.abstractmethod
    def __eq__(self, other):
        return

    @abc.abstractmethod
    def __hash__(self):
        return

    def __ne__(self, other):
        return not self == other

    @abc.abstractmethod
    def __repr__(self):
        return

    @abc.abstractmethod
    def aux(self, n):
        return

    @abc.abstractmethod
    def aux2(self, n):
        return

    def show(self, n):
        return self.aux(n)

    def show2(self, n):
        return self.aux2(n)

    @abc.abstractmethod
    def getVars(self):
        return

    @abc.abstractmethod
    def places(self):
        return

    @abc.abstractmethod
    def constantNumber(self):
        return

    def constantPercentage(self):
        return self.constantNumber()/self.places()

class Node(Tree):

    def __init__(self, l, r):
        self.left = l
        self.right = r
        self.vars = unify(l.vars, r.vars, l.dict)
        self.dict = l.dict = r.dict
        self.size = l.size + r.size

    def instantiate(self, d):
        return Node(self.left.instantiate(d), self.right.instantiate(d))

    def __eq__(self, other):
        return ((isinstance(other, Node)) and (self.vars == other.vars) and 
                (self.dict == other.dict) and (self.degree() == other.degree()) and 
                (self.size == other.size) and (self.left == other.left) and
                (self.right == other.right))

    def __hash__(self):
        return hash((self.vars, self.dict, self.size, self.degree(),
                     self.left, self.right))

    def __repr__(self):
        return self.aux(" ")

    def aux(self, n):
        s = ""
        if self.left:
            s = s + n + "{\n" + self.left.aux(n+"  ") + "\n" + n + "}\n" + n + "  . \n"
            
        if self.right:
            s = s + n + "{\n" + self.right.aux(n+"  ") + "\n"+ n+"}"
        return s

    def show(self, n):
        return self.aux(n)

    def show2(self, n):
        return self.aux2(n)

    def aux2(self, n):
        s = ""
        if self.left:
            s = s + n + "{\n" + self.left.aux2(n+"  ") + "\n" + n + "}\n" + n + "  UNION \n"
            
        if self.right:
            s = s + n + "{\n" + self.right.aux2(n+"  ") + "\n"+ n+"}"
        return s

    def places(self):
        return self.left.places() + self.right.places()

    def constantNumber(self):
        return self.left.constantNumber() + self.right.constantNumber()

    def getVars(self):
        vs = []
        if self.left:
            vs = vs +self.left.getVars()
        if self.right:
            vs = vs +self.right.getVars()
        return vs

def unify (vars0, vars1, dict0):

    vars2 = set(vars0)
    for v in vars1:
       if v in vars2:
           dict0[v] = dict0[v] - 1
           if dict0.has_key(v) and dict0[v] == 0:
              del dict0[v]
              vars2.remove(v)
       else:
           vars2.add(v)
    return vars2

def getDegree(vars0, dict0):

    s = 0
    for v in vars0:
        s = s + dict0[v]
    return s

class Leaf(Tree):
    def __init__(self, s, vs, dc):
        self.vars = vs
        self.dict = dc
        self.size = 1
        self.service = s

    def __hash__(self):
        return hash((self.vars, self.dict, self.size, self.degree(), 
                     self.service))

    def __repr__(self):
        return str(self.service)

    def __eq__(self, other):
        return ((isinstance(other, Leaf)) and (self.vars == other.vars) and 
                (self.dict == other.dict) and (self.degree() == other.degree()) and 
                (self.service == other.service))

    def instantiate(self, d):
        newvars = self.vars - set(d.keys())
        newdict = self.dict.copy()
        for c in d:
            if c in newdict:
                del newdict[c]
        return Leaf(self.service.instantiate(d), newvars, newdict)

    def aux(self, n):
        return self.service.show(n)

    def aux2(self, n):
        return self.service.show2(n)

    def show(self, n):
        return self.aux(n)

    def show2(self, n):
        return self.aux2(n)

    def getInfoIO(self, query):
        subquery = self.service.getTriples()
        vs = self.service.getVars()
        variables = [string.lstrip(string.lstrip(v, "?"), "$") for v in vs]
        if query.args == []:
            projvars = vs
        else:
            projvars = [v.name for v in query.args if not v.constant]
        subvars = list((query.join_vars | set(projvars)) & set(vs))
        subvars = string.joinfields(subvars, " ")
        if query.distinct:
            d = "DISTINCT "
        else:
            d = ""
        subquery = "SELECT "+d+ subvars + " WHERE {" + subquery + "}"
        return (self.service.endpoint, query.getPrefixes()+subquery, set(variables))

    def getCount(self, query, vars):
        subquery = self.service.getTriples()
        if len(vars) == 0:
            vs = self.service.getVars()
            variables = [string.lstrip(string.lstrip(v, "?"), "$") for v in vs]
            vars_str = "*"
        else:
            variables = vars
            service_vars = self.service.getVars()
            vars2 = []
            for v1 in vars:
                for v2 in service_vars:
                    if (v1 == v2[1:]):
                        vars2.append(v2)
                        break
            if len(vars2) > 0:
                vars_str = string.joinfields(vars2, " ")
            else:
                vars_str = "*"

        d = "DISTINCT "
        subquery = "SELECT COUNT "+d+ vars_str + " WHERE {" + subquery + "}"
        return (self.service.endpoint, query.getPrefixes()+subquery)

    def getVars(self):
        return self.service.getVars()

    def places(self):
        return self.service.places()

    def constantNumber(self):
        return self.service.constantNumber()

def sort(lss):

    lo = []
    while not(lss == []):
        m = 0
        for i in xrange(len(lss)):
            if lss[i].constantPercentage() > lss[m].constantPercentage():
                m = i
        lo.append(lss[m])
        lss.pop(m)
    return lo

def createLeafs(lss):

    d = dict()
    for s in lss:
        l = s.getVars()
        l = set(l)
        for e in l:
            d[e] = d.get(e, 0) + 1
    el = []
    for e in d:
        d[e] = d[e] - 1
        if d[e] <= 0:
            el.append(e)
    for e in el:
        del d[e]
    ls = []
    lo = sort(lss)

    for s in lo:
        e = set()
        l = s.getVars()
        for v in l:
            if d.has_key(v):
                e.add(v)
        ls.append(Leaf(s, e, d))

    return (d, ls)

def shareAtLeastOneVar(l, r):

    return len(l.vars & r.vars) > 0

def sortedInclude(l, e):

    for i in range(0, len(l)):
        if e < l[i]:
            l.insert(i, e)
            return
    l.insert(len(l), e)

def makeNode(l, r):

    if l.constantPercentage() > r.constantPercentage():
        n = Node(l, r)
    else:
        n = Node(r, l)
    return n

def makeBushyTree(ss):

    (d, pq) = createLeafs(ss)
    heapq.heapify(pq)
    others = []
    while len(pq) > 1:
        done = False
        l = heapq.heappop(pq)

        lpq = heapq.nsmallest(len(pq), pq)

        for i in range(0, len(pq)):
            r = lpq[i]

            if shareAtLeastOneVar(l,r):
                pq.remove(r)
                n = makeNode(l, r)
                heapq.heappush(pq, n)
                done = True
                break
        if not done: 
            others.append(l)

    if len(pq) == 1:
        for e in others:
            pq[0] = makeNode(pq[0], e)
        return pq[0]
    elif others:
        while len(others) > 1:
            l = others.pop(0)
            r = others.pop(0)

            n = Node(l, r)
            others.append(n)
        if others:
            return others[0]
        return None

def makeNaiveTree(ss):
    (_, pq) = createLeafs(ss)
    while len(pq) > 1:
        l = pq.pop(0)
        r = pq.pop(0)

        n = makeNode(l, r)
        pq.append(n)

    if len(pq) == 1:
        return pq[0]
    else:
        return None

def makeLLTree(ss):

    (_, pq) = createLeafs(ss)
    while len(pq) > 1:
        l = pq.pop(0)
        r = pq.pop(0)

        n = makeNode(l, r)
        pq.insert(0, n)

    if len(pq) == 1:
        return pq[0]
    else:
        return None

