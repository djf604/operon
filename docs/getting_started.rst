Getting Started
===============

To install with pip:
::

    $ pip install operon

Operon keeps track of pipelines and configurations automatically in a hidden directory. Before Operon can be used,
this directory needs to be initialized with the command::

    $ operon init

To install a pipeline::

    $ operon install /path/to/pipeline.py

To configure the pipeline and optionally install software::

    $ operon configure <pipeline-name>

To run the pipeline::

    $ operon run <pipeline-name>

