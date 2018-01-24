Using Operon
============

The primary user interaction with Operon is through the command line interface, exposed as the executable ``operon``.
Subcommands are provided to install, manage, and run pipelines compatible with the Operon framework.

Initialization
^^^^^^^^^^^^^^

Operon keeps track of pipelines and configurations, among other metadata, in a hidden directory. Before Operon can be
used, this directory structure needs to be initialized::

    $ operon init [init-path]

Where ``init-path`` is an optional path pointing to the location where the hidden ``.operon`` folder should be
initialized. If this path isn't given it will default to the user's home directory.

By default, whenever Operon needs to access the ``.operon`` folder it will look in the user's home directory. If the
``.operon`` folder has been initialized elsewhere, there must be a shell environment variable ``OPERON_HOME`` which
points to the directory containing the ``.operon`` folder.

.. note::
    If a path other than the user's home directory is given to the ``init`` subprogram, it will attempt to add the
    ``OPERON_HOME`` environment variable to the user's shell session in ``~/.bashrc``, ``~/.bash_profile``, or
    ``~/.profile``.

After a successful initialization, a new shell session should be started for tab-completion and the ``OPERON_HOME``
environment variable to take effect.

Pipeline Installation
^^^^^^^^^^^^^^^^^^^^^

To install an Operon compatible pipeline into the Operon system::

    $ operon install /path/to/pipeline.py [-y]

The pipeline file will be copied into the Operon system and optionally Python package dependencies, as specified by
the pipeline just installed, can be installed into the current Python environment using ``pip``.

.. caution::
    If ``install`` attempts to install Python package dependencies, it will attempt to do so using the ``--upgrade``
    flag to ``pip``. If in the current Python environment those packages already exist, they will be either upgraded
    or downgraded, which may cause other software to stop functioning properly.

Pipeline Configuration
^^^^^^^^^^^^^^^^^^^^^^

To configure an Operon pipeline with platform-static values and optionally use Miniconda to install software
executables that the pipeline uses::

    $ operon configure <pipeline-name> [-h] [--location LOCATION] [--blank]

If this is the first time the pipeline has been configured and Miniconda is found in ``PATH``, then the ``configure``
subprogram will attempt to create a new conda environment, install software instances that the pipeline uses, then
inject those software paths into the next configuration step. If a conda environment for this pipeline has been
created before, ``configure`` can attempt to inject those software paths instead.

For the configuration step, Operon will ask the user to provide values for the pipeline which will not change from
run to run such as software paths, paths to reference files, etc. The question is followed by a value in brackets
(``[]``), which is the used value if no input is provided. If a conda environment is used, this value in brackets will
be the injected software path.

By default, the configuration file is written into the ``.operon`` folder where it will automatically be called up
when the user runs ``operon run``. If ``--location`` is given as a path, the configuration file will be written
out there instead.

Seeing Pipeline States
^^^^^^^^^^^^^^^^^^^^^^

To see all pipelines in the Operon system and whether each has a corresponding configuration file::

    $ operon list

To see detailed information about a particular pipeline, such as current configuration, command line options, any
required dependencies, etc::

    $ operon show <pipeline-name>

Run a Pipeline
^^^^^^^^^^^^^^

To run an installed pipeline::

    $ operon run <pipeline-name> [--pipeline-config CONFIG] [--parsl-config CONFIG] \
                                 [--logs-dir DIR] [pipeline-options]

The set of accepted ``pipeline-options`` is defined by the pipeline itself and are meant to be values that change from
run to run, such as input files, metadata, etc. Three options will always exist:

* ``pipeline-config`` can point to a pipeline config to use for this run only
* ``parsl-config`` can point to a file containing JSON that represents a Parsl config to use for this run only
* ``logs-dir`` can point to a location where log files from this run should be deposited; if it doesn't exist, it
  will be created; defaults to the currect directory

When an Operon pipeline is run, under the hood it creates a Parsl workflow which can be run in many different ways
depending on the accompanying Parl configuration. This means that while the definition for a pipeline run with the
``run`` subprogram is consistent, that actual execution model may vary if the Parsl configuration varies.

Parsl Configuration
*******************

Parsl is the package the powers Operon and and is responsible for Operon's powerful and flexible parallel execution.
Operon itself is only a front-end abstraction of a Parsl workflow; the actual execution model is fully
Parsl-specific and as such it's advised to check out the
`Parsl documentation <http://parsl.readthedocs.io/en/latest/>`_
to get a sense for how to design a Parls configuration for a specific need-case.

The ``run`` subprogram attempts to pull a Parsl configuration from the user in the following order:

1. From the command line argument ``--parsl-config``
2. From the pipeline configuration key ``parsl_config``
3. From a platform default JSON file located at ``$OPERON_HOME/.operon/parsl_config.json``
4. A default parsl configuration provided by the pipeline
5. A package default parsl configuration of 8 workers using Python threads

For more detailed information, refer to AnotherPage

Command Line Help
^^^^^^^^^^^^^^^^^

All subcommands can be followed by a ``-h``, ``--help``, or ``help`` to get a more detailed explanation for how it
should be used.