=================
Navitron Cronjobs
=================

`Plumbum`_ powered cronjobs for maintaining Navitron backend

Getting Started
===============

    ``pip install -e .`` 

Install project, with dependencies.  `virtualenv`_ use is encouraged.

Accessing Individual Cron Scripts
---------------------------------

All cron scripts are exposed via ``entry_points``.  They should be available by name (no ``.py``) once installed.

    ``navitron_system_kills -v`` 
    
    Runs: ``python NavitronEve/crons/navitron_crons/navitron_system_kills.py -v``

Config Management
-----------------

Each script has a ``--dump-config`` arg.  This can be piped into a new config file and sourced back into scritps with ``--config``.

Please **DO NOT COMMIT SECRETS**

Database Management
-------------------

This project utilizes `MongoDB`_ as a backend.  This allows for direct REST->db dumping of data.  Please see `connections`_ docs for more info.

Cronjobs
========

navitron_maps
-------------

Fetches ``/universe/system_kills/`` from `EVE Online ESI`_ API.  

TODO: MORE INFO

- Args
- Cron recipe

.. _Plumbum: http://plumbum.readthedocs.io/en/latest/cli.html
.. _virtualenv: http://docs.python-guide.org/en/latest/dev/virtualenvs/
.. _MongoDB: https://www.mongodb.com/
.. _connections:
.. _EVE Online ESI: https://esi.tech.ccp.is/latest/