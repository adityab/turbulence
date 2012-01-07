Distributed Database
====================

To deal with the kind of loads expected, Turbulence will have to scale
horizontally.

Requirements for distributed Turbulence:

* Related data should preferably be on one machine.
* Queries will need to be mapped across machines and need recombining.
* Some form of consistent hashing
* Query storage and matching against existing queries as new data comes and
  doing it efficiently.
* Presenting consistent view of data across multiple replicas
* Problems due to locking when multiple writers write and readers read.
