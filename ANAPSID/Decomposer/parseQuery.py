from ply import lex, yacc

from services import Query, Argument, Triple, UnionBlock, JoinBlock, Optional, Filter, Expression

# Lexer

reserved = {
    'UNION' : 'UNION',
    'FILTER' : 'FILTER',
    'OPTIONAL' : 'OPTIONAL',
    'SELECT' : 'SELECT',
    'DISTINCT' : 'DISTINCT',
    'WHERE' : 'WHERE',
    'PREFIX' : 'PREFIX'
}

tokens = [
    "CONSTANT",
    "VARIABLE",
    "LKEY",
    "RKEY",
    "COLON",
    "POINT",
    "URI",
    "ALL",
    "LPAR",
    "RPAR",
    "EQUALS",
    "NEQUALS",
    "LESS",
    "LESSEQ",
    "GREATER",
    "GREATEREQ",
    "ID"#,
#    "PREF",
#    "COLON"
     ] + list(reserved.values())

def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9\-]*'
    t.type = reserved.get(t.value.upper(),'ID')    # Check for reserved words
    return t

t_CONSTANT = r"(\"|\')[^\"\'\n\r]*(\"|\')"
t_VARIABLE = r"([\?]|[\$])([A-Z]|[a-z])\w*"
t_LKEY = r"\{"
t_LPAR = r"\("
t_RPAR = r"\)"
t_COLON = r"\:"
t_RKEY = r"(\.)?\s*\}"
#t_SELECT = r"\S\E\L\E\C\T" +"\s"+r"\D\I\S\T\I\N\C\T"
#t_SELECT = r"[S|s][E|e][L|l][E|e][C|c][T|t]" +"\s"+r"([D|d][I|i][S|s][T|t][I|i][N|n][C|c][T|t])?"
#t_WHERE = r"[W|w][H|h][E|e][R|r][E|e]"
t_POINT = r"\."
#t_PREFIX = r"(PREFIX)|(\prefix)"
#t_PREF = r"[^ \t\n\r\f\v\:]+"
#t_COLON = r"\:"
#t_PRED0 = r"\#[^ \t\n\r\f\v\.](\S)*"+":"+r"(\s)?(\S)+"
#t_PRED1 = r"\#[^ \t\n\r\f\v\.](\S)*"+":"+r"(\s)?<"+"\S+"+r">"
t_EQUALS = r"="
t_NEQUALS = r"\!="
t_LESS = r"<"
t_LESSEQ = r"<="
t_GREATER = r">"
t_GREATEREQ = r">="
t_URI = r"<\S+>"
t_ALL = r"\*"

t_ignore = ' \t\n'

def t_error(t):
    raise TypeError("Unknown text '%s' in line %d " % (t.value,t.lexer.lineno,))

# Define a rule so we can track line numbers
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

# Compute column. 
#     input is the input text string
#     token is a token instance
#def find_column(input,token):
#    last_cr = input.rfind('\n',0,token.lexpos)
#    if last_cr < 0:
#	last_cr = 0
#    column = (token.lexpos - last_cr) + 1
#    return column

lexer = lex.lex()

# Parser

def p_parse_sparql(p):
    """
    parse_sparql : prefix_list query
    """
    (vs, ts, d) = p[2]
    p[0] = Query(p[1], vs, ts, d)

def p_prefix_list(p):
    """
    prefix_list : prefix prefix_list
    """
    p[0] = [p[1]] + p[2]

def p_empty_prefix_list(p):
    """
    prefix_list : empty
    """
    p[0] = []

def p_empty(p):
    """
    empty :
    """
    pass

def p_prefix(p):
    """
    prefix : PREFIX uri
    """
    p[0] = p[2]

#def p_pref(p):
#    """
#    pref : uri
#    """
#    p[0] = p[1]

def p_uri_0(p):
    """
    uri : ID COLON ID
    """
    p[0] = p[1]+p[2]+p[3]

def p_uri_1(p):
    """
    uri : ID COLON URI
    """
    p[0] = p[1]+p[2]+p[3]

def p_uri_2(p):
    """
    uri : URI
    """
    p[0] = p[1]

def p_query_0(p):
    """
    query : SELECT distinct var_list WHERE LKEY group_graph_pattern RKEY
    """
    p[0] = (p[3], p[6], p[2])

def p_query_1(p):
    """
    query : SELECT distinct ALL WHERE LKEY group_graph_pattern RKEY
    """
    p[0] = ([], p[6], p[2])

def p_distinct_0(p):
    """
    distinct : DISTINCT
    """
    p[0] = True

def p_distinct_1(p):
    """
    distinct : empty
    """
    p[0] = False

def p_ggp_0(p):
    """
    group_graph_pattern : union_block
    """
    p[0] = UnionBlock(p[1])

def p_union_block_0(p):
    """
    union_block : join_block rest_union_block
    """
    p[0] = [JoinBlock(p[1])] + p[2]

def p_rest_union_block_0(p):
    """
    rest_union_block : empty
    """
    p[0] = []

def p_rest_union_block_1(p):
    """
    rest_union_block : UNION join_block rest_union_block
    """
    p[0] = [JoinBlock(p[2])] + p[3]

def p_join_block(p):
    """
    join_block : bgp rest_join_block 
    """
    p[0] = [p[1]] + p[2]

def p_rest_join_block_0(p):
    """
    rest_join_block : empty
    """
    p[0] = []

def p_rest_join_block_1(p):
    """
    rest_join_block : POINT bgp rest_join_block
    """
    p[0] = [p[2]]+p[3]

def p_bgp_0(p):
    """
    bgp : triple
    """
    p[0] = p[1]

def p_bgp_1(p):
    """
    bgp : FILTER LPAR expression RPAR
    """
    p[0] = Filter(p[3])

def p_bgp_2(p):
    """
    bgp : OPTIONAL LKEY group_graph_pattern RKEY
    """
    p[0] = Optional(p[3])

def p_bgp_3(p):
    """
    bgp : LKEY group_graph_pattern RKEY
    """
    p[0] = p[2]

def p_var_list(p):
    """
    var_list : var_list VARIABLE
    """
    p[0] = p[1] + [Argument(p[2], False)]

def p_single_var_list(p):
    """
    var_list : VARIABLE
    """
    p[0] = [Argument(p[1], False)]
    
#def p_triple_list(p):
#    """
#    triple_list : triple_list POINT triple
#    """
#    p[0] = p[1] + [p[3]]

#def p_single_triple_list(p):
#    """
#    triple_list : triple
#    """
#    p[0] = [p[1]]

def p_triple_0(p):
    """
    triple : subject predicate object
    """
    p[0] = Triple(p[1], p[2], p[3])

def p_expression_0(p):
    """
    expression : expression EQUALS expression
    """
    p[0] = Expression(p[2], p[1], p[3])

def p_expression_1(p):
    """
    expression : CONSTANT
    """
    p[0] = Argument(p[1], True)

def p_expression_2(p):
    """
    expression : VARIABLE
    """
    p[0] = Argument(p[1], False)

def p_expression_3(p):
    """
    expression : expression LESS expression
    """
    p[0] = Expression(p[2], p[1], p[3])

def p_expression_4(p):
    """
    expression : expression LESSEQ expression
    """
    p[0] = Expression(p[2], p[1], p[3])

def p_expression_5(p):
    """
    expression : expression GREATER expression
    """
    p[0] = Expression(p[2], p[1], p[3])

def p_expression_6(p):
    """
    expression : expression GREATEREQ expression
    """
    p[0] = Expression(p[2], p[1], p[3])

def p_expression_7(p):
    """
    expression : expression NEQUALS expression
    """
    p[0] = Expression(p[2], p[1], p[3])

#def p_expression_3(p):
#    """
#    expression : expression RAB expression
#    """
#    p[0] = Expression(p[2], p[1], p[3])

#def p_expression_4(p):
#    """
#    expression : expression LAB expression
#    """
#    p[0] = Expression(p[2], p[1], p[3])

def p_predicate_uri(p):
    """
   predicate : uri
    """
    p[0] = Argument(p[1], True)

def p_predicate_var(p):
    """
   predicate : VARIABLE
    """
    p[0] = Argument(p[1], False)

def p_subject_uri(p):
    """
    subject : uri
    """
    p[0] = Argument(p[1], True)

def p_subject_variable(p):
    """
    subject : VARIABLE
    """
    p[0] = Argument(p[1], False)

def p_object_uri(p):
    """
    object : uri
    """
    p[0] = Argument(p[1], True)

def p_object_variable(p):
    """
    object : VARIABLE
    """
    p[0] = Argument(p[1], False)

def p_object_constant(p):
    """
    object : CONSTANT
    """
    p[0] = Argument(p[1], True)

def p_error(p):
        raise TypeError("unknown text at %r" % (p.value,))

parser = yacc.yacc(debug=0)

# Helpers

def parse(string):

    return parser.parse(string, lexer=lexer)
