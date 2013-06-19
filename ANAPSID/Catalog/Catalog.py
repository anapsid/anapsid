'''
Created on Jul 10, 2011

The catalog maintains a list of the available endpoints and
their ontology concepts.

All this information is read from an input file, called "filename". 

@author: Maribel Acosta Deibe
'''

class Catalog(object):

    def __init__(self, filename):
        
        self.data = dict()
        
        for line in open(filename,"r").readlines():
            linelist = line.split("|")
            if (len(linelist)>2):
                self.data.update({linelist[0]:linelist[1:len(linelist)-1]}) 
        