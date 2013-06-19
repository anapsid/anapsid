ANAPSID
=======

An adaptive query processing engine for SPARQL endpoints.

Installing ANAPSID
==================

ANAPSID is known to run on Debian GNU/Linux. The recommended way to
install ANAPSID is to use Python 2.7 through VirtualEnv. Follow these
instructions to get a working ANAPSID install:

1. Install VirtualEnv. The easiest way to do this is installing
   [VirtualEnv Burrito](https://github.com/brainsik/virtualenv-burrito).

2. Once you install VirtualEnv Burrito, create a new VirtualEnv for
   your ANAPSID install:
   `$ mkvirtualenv anapsid`

3. Clone this repository using Git. Go to your local copy of ANAPSID
   and run:
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

   `$ get_predicates endpointsURLs.txt > endpointsDescriptions.txt`

3. You are ready to run ANAPSID.

About supported endpoints
------------------------

ANAPSID currently supports endpoints that answer queries either on XML
or JSON. Expect hard failures if you intend to use ANAPSID on
endpoints that answer in any other exotic format.

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
2. Star-Shaped Group Single endpoint selection (SSGS).
3. Star-Shaped Group Multiple endpoint selection (SSGM).

About and Contact
=================

ANAPSID was developed at
[Universidad Simón Bolívar](http://www.usb.ve) as an ongoing academic effort. You
can contact the current maintainers by email at anapsid[at]ldc[dot]usb[dot]ve.

We strongly encourage you to please report any issues you have with
ANAPSID. You can do that over our contact email or creating a new
issue here on Github.

License
=======

This work is licensed under [GNU/GPL v2](https://www.gnu.org/licenses/gpl-2.0.html).
