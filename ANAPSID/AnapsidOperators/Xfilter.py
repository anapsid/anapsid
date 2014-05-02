'''
Created on Mar 03, 2014

Implements the Xfilter operator.
The intermediate results are represented in a queue. 

@author: Maribel Acosta Deibe
'''
from multiprocessing import Queue
from ANAPSID.Decomposer.services import Filter, Expression, Argument
import datetime
import operator

unary_operators = {
        '!'  : operator.not_,
        '+'  : '',
        '-'  : operator.neg
        }

logical_connectives = {
        '||' : operator.or_,
        '&&' : operator.and_
        }

arithmetic_operators = {
        '*'  : operator.mul,
        '/'  : operator.div,
        '+'  : operator.add,
        '-'  : operator.sub, 
         }

test_operators = {
        '='  : operator.eq,
        '!=' : operator.ne,
        '<'  : operator.lt,
        '>'  : operator.gt,
        '<=' : operator.le,
        '>=' : operator.ge
        }

data_types = {
        'integer' : (int, 'numerical'),
        'decimal' : (float, 'numerical'),
        'float'   : (float, 'numerical'),
        'double'  : (float, 'numerical'),
        'string'  : (str, str),
        'boolean' : (bool, bool),
        'dateTime' : (datetime, datetime),
        'nonPositiveInteger' : (int, 'numerical'),
        'negativeInteger' : (int, 'numerical'),
        'long'    : (long, 'numerical'),
        'int'     : (int, 'numerical'),
        'short'   : (int, 'numerical'),
        'byte'    : (bytes, bytes),
        'nonNegativeInteger' : (int, 'numerical'),
        'unsignedLong' : (long, 'numerical'),
        'unsignedInt'  : (int, 'numerical'),
        'unsignedShort' : (int, 'numerical'),
        'unsignedByte' : (bytes, bytes), # TODO: this is not correct
        'positiveInteger' : (int, 'numerical')
        }

numerical = (int, long, float)

class Xfilter(object):
    
    def __init__(self, filter):
        self.input = Queue()
        self.qresults = Queue()
        self.filter = filter
        
        
    def execute(self, left, dummy, out):
        # Executes the Xfilter.
        self.left = left
        self.qresults = out
        #print "self.filter.expr.op", self.filter.expr.op
        #print "self.filter.expr.left", self.filter.expr.left
        #print "self.filter.expr.right", self.filter.expr.right
        # Apply filter tuple by tuple. 
        tuple = self.left.get(True)
        while (tuple != "EOF"):
            (res, _) = self.evaluateComplexExpression(tuple, self.filter.expr.op,
                                              (self.filter.expr.left, None), (self.filter.expr.right, None))            

            if res:
                self.qresults.put(tuple)    
                
            tuple = self.left.get(True)

        #print "out of while cycle in filter" 

        # Put EOF in queue and exit. 
        self.qresults.put("EOF")
        #return
   
   
    # Base case.   
    def evaluateOperator(self, operator, expr_left, expr_right):
        #print "operator in Filter", operator 
        if (operator in unary_operators):
            #print "Case: unary_operators"
            return self.evaluateUnaryOperator(operator, expr_left)
        elif (operator in logical_connectives):
            #print "Case: logical connectives"
            return self.evaluateLogicalConnective(operator, expr_left, expr_right)
        elif (operator in arithmetic_operators):
            #print "Case: arithmetic operator"
            return self.evaluateArithmeticOperators(operator, expr_left, expr_right)
        elif (operator in test_operators):
            #print "Case: test"
            return self.evaluateTest(operator, expr_left, expr_right)
            
    
    # Inductive case.
    def evaluateComplexExpression(self, tuple, operator, (expr_left,type_left), (expr_right,type_right)):
        
        # Case 1: Inductive case binary operator OP(Expr, Expr)
        if isinstance(expr_left, Expression) and isinstance(expr_right, Expression):
            #print "Case 1"
            res_left = self.evaluateComplexExpression(tuple, expr_left.op, 
                                                      (expr_left.left,type_left), (expr_left.right, type_right))
            res_right = self.evaluateComplexExpression(tuple, expr_right.op,
                                                       (expr_right.left,type_left), (expr_right.right,type_right))
            res = self.evaluateOperator(operator, res_left, res_right)
        
        # Case 2: Inductive case binary operator OP(Expr, Arg)
        elif isinstance(expr_left, Expression) and isinstance(expr_right, Argument):
            #print "Case 2"
            res_left = self.evaluateComplexExpression(tuple, expr_left.op,
                                                      (expr_left.left,type_left), (expr_left.right,type_right))
            res_right = self.extractValue(tuple[expr_right.name[1:]])
            res = self.evaluateOperator(operator, res_left, res_right)

       # Case 3: Inductive case binary operator OP(Arg, Expr)
        elif isinstance(expr_left, Argument) and isinstance(expr_right, Expression):
            #print "Case 3"
            res_left = self.extractValue(tuple[expr_left.name[1:]])
            res_right = self.evaluateComplexExpression(tuple, expr_right.op,
                                                       (expr_right.left,type_left), (expr_right.right,type_right))
            res = self.evaluateOperator(operator, res_left, res_right)

        # Case 4: Inductive case unary operator OP(Expr, None)
        elif isinstance(expr_left, Expression):
            #print "Case 4"
            res_left = self.evaluateComplexExpression(tuple, expr_left.op,
                                                      (expr_left.left,type_left), (expr_left.right,type_right))
            res = self.evaluateOperator(operator, res_left, None)
            
        # Case 5: Base case binary operator OP(Arg, Arg)
        elif isinstance(expr_left, Argument) and isinstance(expr_right, Argument):
            #print "Case 5"
            res_left = self.extractValue(tuple[expr_left.name[1:]])
            res_right = self.extractValue(tuple[expr_right.name[1:]])
            res = self.evaluateOperator(operator, res_left, res_right)
        
        # Case 6: Base case unary operator OP(Arg, None)
        elif isinstance(expr_left, Argument):
            #print "Case 6"
            res_left = self.extractValue(tuple[expr_left.name[1:]])
            res = self.evaluateOperator(operator, res_left, None)  
        else:
            pass   
 
        return res

    '''
    evaluateEBV: calculates whether an argument is an Effective Boolean Value (EBV)
                 according to the definition in the SPARQL documentation 
                 See: http://www.w3.org/TR/sparql11-query/#ebv
    
    input: val -- an argument
    return: (isEBV, EBV) -- both of Python type bool
    '''    
    def evaluateEBV(self, casted_val, type_val):

        # Handles python data types.
        if (isinstance(casted_val, bool)):
            return (True, casted_val)
        if (isinstance(casted_val, numerical)):
            if (casted_val == 0 or casted_val == 'nan'):
                return (True, False)
            else:
                return (True, True)

        # Rule 1
        if ((type_val == bool) and (casted_val != 'true') and (casted_val != 'false')):
            return (True, False)
        elif ((type_val == 'numeric') and not(isinstance(casted_val, numerical))):
            return (True, False)

        # Rule 2
        if (type_val == bool):
            if (casted_val == 'true'):
                return (True, True)
            elif (casted_val == 'false'):
                return (True, False)

        # Rule 3
        if (type_val == str):
            if (len(casted_val) == 0):
                return (True, False)
            else:
                return (True, True)

        # Rule 4
        if ((type_val == 'numeric')):
            if (casted_val == 0 or casted_val == 'nan'):
                return (True, False)
            else:
                return (True, True)
     
        # Rule 5: The error type should be raised by the evaluators.
        return (False, None)
    
    
    def evaluateUnaryOperator(self, tuple, operator, (expr_left, type_left)):
 
        if (operator == '+' and isinstance(expr_left, numerical)):
            return (expr_left, type_left)
 
        elif (operator == '-' and isinstance(expr_left, numerical)):
            return (unary_operators[operator](expr_left), type_left)

        elif (operator == '!'):
            (isEBV, ebv) = self.evaluateEBV(expr_left, type_left)
            if (isEBV):
                return (unary_operators[operator](ebv), type_left)
            else:
                raise SPARQLTypeError
        else:
            raise SPARQLTypeError
    
    
    def evaluateLogicalConnective(self, operator, (expr_left, type_left), (expr_right, type_right)):

        (isEBV_left, ebv_left) = self.evaluateEBV(expr_left, type_left)
        (isEBV_right, ebv_right) = self.evaluateEBV(expr_right, type_right)

        #print "in evaluateLogicalConnective", expr_left, isEBV_left, ebv_left
        #print "in evaluateLogicalConnective", expr_right, isEBV_right, ebv_right

        if (isEBV_left and isEBV_right):
            return (logical_connectives[operator](ebv_left, ebv_right), bool)

        elif (isEBV_left):
            res = logical_connectives[operator](ebv_left, 'Error')
            if (res == 'Error'):
                raise SPARQLTypeError
            else:
                return (res, bool)

        elif (isBV_right):
            res = logical_connectives[operator](ebv_right, 'Error')
            if (res == 'Error'):
                raise SPARQLTypeError
            else:
                return (res, bool)
    
    def evaluateTest(self, operator, (expr_left, type_left), (expr_right, type_right)):
                
        if ((type(expr_left) == type(expr_right)) or (isinstance(expr_left, numerical) and isinstance(expr_right, numerical))):
            #print "Here", val_left, type_left, val_right, type_right
            return (test_operators[operator](expr_left, expr_right), bool)
        else:
            print "SPARQLTypeError"
            raise SPARQLTypeError
    
    
    def evaluateAritmethic(self, operator, (expr_left, type_left), (expr_right, type_right)):
        
        if (isinstance(expr_left, numerical) and isinstance(expr_right, numerical)):
            return (arithmetic_operators[operator](expr_left, expr_right), type_left) # TODO: implement the cases with types
        else:
            raise SPARQLTypeError

     
    def extractValue(self, val):
        pos = val.find("^^")
    
        # Handles when the literal is typed.
        if (pos > -1):
            for t in data_types.keys():
                if (t in val[pos]):
                    (python_type, general_type) = data_types[t]

                    if (general_type == bool):
                        return (val[:pos], general_type)
	
                    else:
                        return( python_type(val[:pos]), general_type)
            
        # Handles non-typed literals.
        else:
            return (str(val), str)
        
        
class SPARQLTypeError(Exception):
    """Base class for exceptions in this module."""
    pass
