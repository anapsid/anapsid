ANAPSID
=======

An adaptive query processing engine for SPARQL endpoints.

[1] Maribel Acosta, Maria-Esther Vidal, Tomas Lampo, Julio Castillo,
Edna Ruckhaus: ANAPSID: An Adaptive Query Processing Engine for SPARQL
Endpoints. International Semantic Web Conference (1) 2011: 18-34

[2] Gabriela Montoya, Maria-Esther Vidal, Maribel Acosta: A
Heuristic-Based Approach for Planning Federated SPARQL Queries. COLD
2012

Installing ANAPSID
==================

ANAPSID is known to run on Debian GNU/Linux and OS X. These instructions were tested 
on the latest Debian Stable and OS X. The recommended way to
execute ANAPSID is to use Python 2.7. 


1. Download ANAPSID.

   You can do this by cloning this repository using Git.

   `$ git clone https://github.com/anapsid/anapsid.git ~/anapsid`
   
   OR

   You can download the latest release from Github [here](https://github.com/anapsid/anapsid/releases) 

2. Go to your local copy of ANAPSID and run:

   `$ pip install -r requirements.txt`

   This will install ANAPSID's Python dependencies. Right now, the only library required to execute ANAPSID is ply 3.3 (https://pypi.python.org/pypi/ply/3.3)

3. When step 2 is done you can now install ANAPSID. This will install
   it only to your current user caged VirtualEnv as to prevent
   polluting Python's global site-packages.

   `$ python setup.py install`

4. Go ahead and move to the next section on configuring ANAPSID.

Setting up ANAPSID
==================

Running ANAPSID depends on a endpoint description file. This file
describes each endpoint URL and the predicates this endpoint
handles. ANAPSID comes bundled with a helper script to generate your
endpoints descriptions as to prevent errors.

1. Create a file, e.g. endpointsURLs.txt, with the URLs of your
   endpoints, one per line.

2. Run the script. It will contact each endpoint and retrieve their
   predicates, so it might take a while. This will save your endpoint
   descriptions on endpointsDescriptions.txt

   `$ get_predicates endpointsURLs.txt endpointsDescriptions.txt`

3. You are ready to run ANAPSID.

About supported endpoints
------------------------

ANAPSID currently supports endpoints that answer queries either on XML
or JSON. Expect hard failures if you intend to use ANAPSID on
endpoints that answer in any other format.

Running ANAPSID
===============

Once you have installed ANAPSID and retrieved endpoint descriptions,
you can run ANAPSID using our run_anapsid script.

`$ run_anapsid`

It will output a usage text and the options switches you can
select. We run our experiments, however, using the scripts bundled on
utils/ so you might want to check that out to get an idea.

ANAPSID Parameters
------------------
Alternatively,  you can execute the following command to run a given query with ANAPSID:

```
$python $ANAPSIDROOT/run_anapsid -e $ENDPOINTS -q $query -p <planType> -s False 
-o False -d <TypeofDecompostion> -a True -w False [-k <special>]  [-V <typeOfEndpoint>] -r False
```

Where:

`$ANAPSIDROOT`: directory where ANAPSID is stored.

`$ENDPOINTS`: path and name of the file where the description of the endpoints is stored.

`$query`: path and name of the filw where the query is stored.

`<planType>`: can be **b** if the plan is bushy, **ll** is the plan is left linear, and **naive** for naive binary tree plan. 

`-o`: can be True or False. **True** indicates that the input query is in SPARQL1-1 and no decomposition is needed; **False**, otherwise.

`-d`: indicates the type of Decomposition. <TypeofDecompostion> can be **SSGM** (Star Shaped
Group Multiple Endpoints), **SSGS** (Star Shaped Group Single Endpoint), **EG** (Exclusive Groups), Recommended SSGM.

`-a`: indicates if the adaptive operators will be used. Recommended value **True**.

`-w`:  can be True or False. Indicates if the cardinality of the queries will be estimated by contacting the sources (**True**) or by using a cost model (**False**). Turning True this feature may affect execution time.

`-w`: can be y or c. The value **y** indicates that the plan will be produced, while **c** asks that decomposition. This parameter is optional, and should be set up only if the plan of the query wants to be produced. 

`-r`: can be True or False. Use **True** if the answer of the query will be output and **False** if only a summary of the execution will be produced. 

`-V`: can be True or False. **True** indicates if the endpoints to contact are Virtuoso, **False** is of any other type, e.g., OWLIM.


Included query decomposing heuristics
=====================================

We include three heuristics used for decomposing queries to be
evaluated by a federation of endpoints. These are:

1. Exclusive Groups (EG).
2. Star-Shaped Group Single endpoint selection (SSGS). See [2].
3. Star-Shaped Group Multiple endpoint selection (SSGM). See [2].
 
Running FedBench with ANAPSID
=============================
FedBench (see http://fedbench.fluidops.net) is a benchmark for testing federated query processing on RDF data sets.

In order to execute ANAPSID, it is necessary first to provide the endpoint descriptions. Endpoint descriptions are of the form `<URLEndpoint> <LISTOfPredicates>`. The file endpoints/endpointsFedBench provides 
the description of the endpoints of the dataset collections in FedBench. The current URLs of the endpoints
have to be included as follows:
```
 <http://URLnytimes_dataset/sparql> URL of the NYTime endpoint
 <http://URLchebi_dataset/sparql> URL of the Chebi endpoint
 <http://URLSWDF_dataset/sparql> URL of the SW Dog Food endpoint
 <http://URLdrugbank_dataset/sparql> URL of the Drugbank endpoint
 <http://URLjamendo_dataset/sparql> URL of the Jamendo endpoint
 <http://URLkegg_dataset/sparql> URL of the Kegg endpoint
 <http://URLlinkedmdb_dataset/sparql> URL of the LinkedMDB endpoint
 <http://URLSP2B/sparql> URL of the SP^2Bench 10M endpoint
 <http://URLgeonames/sparql> URL of the Geonames endpoint
```

The FedBench queries (see http://fedbench.fluidops.net/resource/Queries) are also available in the folder queries/fedbBench.  

About and Contact
=================

ANAPSID was developed at
[Universidad Simón Bolívar](http://www.usb.ve) as an ongoing academic effort. You
can contact the current maintainers by email at mvidal[at]ldc[dot]usb[dot]ve.

We strongly encourage you to please report any issues you have with
ANAPSID. You can do that over our contact email or creating a new
issue here on Github.

- Simón Castillo: scastillo [at] ldc [dot] usb [dot] ve
- Guillermo Palma: gpalma [at] ldc [dot] usb [dot] ve
- Maria-Esther Vidal: mvidal [at] ldc [dot] usb [dot] ve
- Gabriela Montoya: Gabriela [dot] Montoya [at] univ-nantes [dot] fr
- Maribel Acosta: maribel [dot] acosta [at] kit [dot] edu


License
=======

This work is licensed under [GNU/GPL v2](https://www.gnu.org/licenses/gpl-2.0.html).
