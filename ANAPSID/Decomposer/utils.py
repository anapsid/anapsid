import time
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
