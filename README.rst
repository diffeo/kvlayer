py-skeleton
===========

This contains the central versions of files that are common across
most (maybe all) of Diffeo's python modules/repos.

In order for your various setup.py install_requires lists to find
packages like bigtree, your ~/.pydistutils.cfg file needs to have
this:

[easy_install]
index_url  = http://devhub.diffeo.com:8080/simple/

