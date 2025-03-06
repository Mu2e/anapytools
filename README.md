# anapytools
Python analysis tools for Mu2e collaborators, primarily designed for use on the Fermilab Elastic Analysis Facility (EAF). 

Please see the wiki for information on how to use these tools: https://mu2ewiki.fnal.gov/wiki/Elastic_Analysis_Facility_(EAF)#anapytools.

## sgrant/integration branch

`anapytools` should be a standalone library that provides the ability to run analysis on the gpvms and EAF with minimal overhead. 

It should provide functionality to: 

- Efficiently load files and entire datasets across platforms;
- Access to the full suite of `pyutils` for `EventNtuple` handling; 
- Be the primary platform for Python utilities development. 

As a part of this, I would like to build on `pyutils` by providing tools for importing EventNtuple in a way where it is straightforward to emplace cuts accross subsytem branches. 