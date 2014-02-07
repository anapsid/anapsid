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

ANAPSID is known to run on Debian GNU/Linux. These instructions were tested 
on the latest Debian Stable.  The recommended way to
install ANAPSID is to use Python 2.7 through VirtualEnv. 

At the very least, you will be needing a python interpreter and someway 
to download the files. To satisfy the basic dependencies install on Debian: 

`$ sudo apt-get install python build-essential curl git`

If you cannot install CURL, we provide work arounds using wget. You can ignore the git
dependency and download one of the releases from Github. The python
and build-essential deps are required.

Follow these instructions to get a working ANAPSID install:


1. Install VirtualEnv. The easiest way to do this is installing
   [VirtualEnv Burrito](https://github.com/brainsik/virtualenv-burrito).

   Using CURL:
   
   `curl -s https://raw.github.com/brainsik/virtualenv-burrito/master/virtualenv-burrito.sh | $SHELL`
   
   Using WGET:
   
   `wget https://raw2.github.com/brainsik/virtualenv-burrito/master/virtualenv-burrito.sh && sed -i 's/curl/wget -qO-/g' virtualenv-burrito.sh && bash virtualenv-burrito.sh`

2. Once you install VirtualEnv Burrito, create a new VirtualEnv for
   your ANAPSID install:

   `$ mkvirtualenv anapsid --no-site-packages`

   Once you run this command you will be inside a dedicated VirtualEnv
   for ANAPSID. It should automatically append (anapsid) at the beginning
   of your command line. The only way to execute ANAPSID related commands
   is by entering this VirtualEnv. You can do so, in the future by running:

   `$ workon anapsid`

   To exit the ANAPSID VirtualEnv, just run:

   `$ deactivate`

   From now on, we assume you are working inside the ANAPSID VirtualEnv.

3. Download ANAPSID.

   You can do this by cloning this repository using Git.

   `$ git clone https://github.com/anapsid/anapsid.git ~/anapsid`
   
   OR

   You can download the latest release from Github [here](https://github.com/anapsid/anapsid/releases) 

4. Go to your local copy of ANAPSID and run:

   `$ pip install -r requirements.txt`

   This will install ANAPSID's Python dependencies.

4. When step 3 is done you can now install ANAPSID. This will install
   it only to your current user caged VirtualEnv as to prevent
   polluting Python's global site-packages.

   `$ python setup.py install`

5. Go ahead and move to the next section on configuring ANAPSID.

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

Included query decomposing heuristics
=====================================

We include three heuristics used for decomposing queries to be
evaluated by a federation of endpoints. These are:

1. Exclusive Groups (EG).
2. Star-Shaped Group Single endpoint selection (SSGS). See [2].
3. Star-Shaped Group Multiple endpoint selection (SSGM). See [2].

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
