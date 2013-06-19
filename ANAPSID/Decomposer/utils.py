import time
import pydot
from multiprocessing import Queue

def prefix(p):
    s = p.name
    pos = s.find(":")
    if (not (s[0] == "<")) and pos > -1:
        return (s[0:pos].strip(), s[(pos+1):].strip())

    return None

def getUri(p, prefs):
    hasPrefix = prefix(p)
    if hasPrefix:
        (pr, su) = hasPrefix
        n = prefs[pr]
        n = n[:-1]+su+">"
        return n
    return p.name

def search(l, pred, prefs):
    """Returns the list of endpoints that provides a predicate
    'pred'"""
    r = []
    p = getUri(pred, prefs)
    for (epn, epl) in l:
        if p in epl or (not pred.constant):
            r.append(epn)

    return r

def getQuery(ts, ps):
    q = "ASK { "
    for t in ts:
        p = getUri(t.predicate, ps)
        s = getUri(t.subject, ps)
        o = getUri(t.theobject, ps)
        q = q + s + " " + p + " " + o + " ."
    q = q + " }"
    return q


def test(endpoint, triples, ps, c):
    """Test whether or not this `endpoint` can eval `triples`"""
    query = getQuery(triples, ps)
    server = endpoint[1:-1]
    q = Queue()
    b = c(server, query, q)
    return b

def count(endpoint, triples, ps, c):
    query = "select COUNT(*) {\n%s\n}"
    l = '\n'.join('%s %s %s .' % (getUri(t.subject, ps),
                                  getUri(t.predicate, ps),
                                  getUri(t.theobject, ps))
                  for t in triples)
    server = endpoint[1:-1]
    q = Queue()
    b = c(server, query, q)
    return b

# used for generating graph topology image
def write_graph(graph, triplets):
    g = pydot.Dot(graph_type='graph')
    added = [[False for j in range(len(triplets))] for i in range(len(triplets))]
    nodes = [pydot.Node('T%d %s ' % (i, ', '.join(triplets[i].getVars())))
             for i, v in enumerate(graph)]
    for x in nodes:
        g.add_node(x)
    for i,v in enumerate(graph):
        for j in v:
            if not added[i][j] or not added[j][i]:
                g.add_edge(pydot.Edge(nodes[i], nodes[j]))
                added[i][j] = True
                added[j][i] = True
    g.write_png('graph_topology.%s.png' % str(time.time()))

# colored graph topology
colors = ['#A200FF', '#FF0097', '#00ABA9', '#8CBF26', '#A05000', '#E671B8',
          '#F09609', '#1BA1E2', '#E51400', '#339933']
def color_graph(filename, graph, triplets, services, sub):
    g = pydot.Dot(graph_type='graph')
    added = [[False for j in range(len(triplets))] for i in range(len(triplets))]
    color = {}
    # G = pgv.Graph("Mi grafo")
    cur_color = 0
    for s in services:
        for y in s.triples:
            color[y] = cur_color
        cur_color +=1
    for ub in sub:
        try:
            for y in ub.triples[0].triples[0].triples:
                color[y] = cur_color
        except TypeError:
            color[ub.triples[0].triples[0].triples] = cur_color
        cur_color += 1

    nodes = [pydot.Node('T%d %s ' % (i, ', '.join(triplets[i].getVars())),
                        style='filled', fillcolor=colors[color[triplets[i]]])
             for i,v in enumerate(graph)]
    for x in nodes:
        g.add_node(x)
    for i,v in enumerate(graph):
        for j in v:
            if not added[i][j] or not added[j][i]:
                g.add_edge(pydot.Edge(nodes[i], nodes[j]))
                added[i][j] = True
                added[j][i] = True
    g.write_png('%s.%s.png' % (filename, str(time.time())))
