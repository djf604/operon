Building Pipelines
==================

Dependency Workflow Concepts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Operon pipelines don't actually run anything at all; instead, they define a workflow.

There are two main components to a dependency workflow graph:

1. Executables
2. Data

Executables generally take input, perform some computational work on that input, then produce some output. Data is any
file on disk that is used as input or produced as output and that needs to be considered in the workflow graph.

When building a pipeline in Operon, the connections between Executables and Data need not be a part of the design
process. The developer needs only to define Executables which are a part of the workflow, and the input and/or output
files for each Executable. At runtime, Operon will examine the dependency workflow graph and feed the connections
appropriately into Parsl.

As an example, consider the following scenario in a bioinformatics workflow : a FASTQ file is used as input to an
aligner (say, bwa), which produces a BAM file. That produced BAM file needs to be used as input to two programs, one
to gather flagstats and one to quantify gene counts. The dependency workflow graph would look as follows:


.. image:: _static/flowchart.svg
    :width: 50%
    :align: center

|
The developer for this pipeline only needs to define the following:

* There's a program called bwa, which lives at ``/path/to/bwa``. As input, it takes in a file located at
  ``/path/to/fastq``, and as output it generates a file called ``/path/to/bam``.
* There's a program called samtools, which lives at ``/path/to/samtools``. As input, it takes in a file located at
  ``/path/to/bam``, and as output it generates a file called ``/path/to/flagstats``.
* There's a program called genecounter, which lives at ``/path/to/genecounter``. As input, it takes in a file located
  at ``/path/to/bam``, and as output it generates a file called ``/path/to/genecounts``.

This defines three Software instances (Apps in Parsl verbage): bwa, samtools, and genecounter. All three Software
instances have data dependencies; that is, they are require input data to run. However, two of the Software instances'
data dependencies are not yet available (because they havne't been produced by the pipeline yet), so they will not
run until those dependencies become available. The Software bwa, however, has all of its data dependencies available,
so it begins running immediately. Once bwa is finished running, and consequently produces its output ``/path/to/bam``,
the Software samtools and genecounter both recognize that their data dependencies are now available, and so both
begin running concurrently.

.. note::

    ``/path/to/fastq``, ``/path/to/bwa``, etc are placeholders in the above example. In a real pipeline, the
    developer would gather those values either from the command line via ``pipeline_args`` or from the configuration
    via ``pipeline_config``. As long as ``Data()`` inputs and outputs resolve to filesystem paths at the time of
    workflow generation, Parsl will be able to correctly determine data dependencies.

Pipeline Meta Definitions
^^^^^^^^^^^^^^^^^^^^^^^^^
Pipeline meta definitions describe how the pipeline should be installed, provisioned, and configured so that as little
as possible needs to be done by the user before the pipeline is ready to run on the user's platform.

All pipeline meta definitions (and logic, for that matter) is defined in a single document with a single class, always
called ``Pipeline``, which subclasses ``operon.components.ParslPipeline``.

.. code-block:: python

    from operon.components import ParslPipeline

    class Pipeline(ParslPipeline):
        def description(self):
            return 'An example pipeline'

        ...

        def pipeline(self, pipeline_args, pipeline_config):
            # Pipeline logic here

Description
###########
The description of the pipeline is a string meant to be a human readable overview of what the pipeline does and any
other relevant information for the user.

.. code-block:: python

    def description(self):
        return 'An example pipeline, written in Operon, powered by Parsl'

The pipeline description is displayed when the user runs ``operon show`` or when ``--help`` is given to ``operon run``.
TODO I'm actually not sure about that last one

Dependencies
############

Pipeline dependencies are Python packages which the pipeline logic use. Dependencies are provided as a list of strings,
where each string is the name of a package available on PyPI and suitable to be feed directly into ``pip``.

.. code-block:: python

    def dependencies(self):
        return [
            'pysam==0.13',
            'pyvcf'
        ]

Upon pipeline installation, the user is given the option to use ``pip`` to install dependencies into their current
Python environment. While this may be convenient, it may also cause package collisions or unecessary muddying of a
distribution Python environment, so the user can instead opt to get the dependencies from ``operon show`` and install
them manually into a Python virtual environment.

.. note::

    If the user accepts auto-installing dependencies into their current Python environment, ``pip`` will attempt to
    do so using the ``--upgrade`` flag. This may upgrade or downgrade packages already installed in the current
    Python environment if there are any collisions.

Conda/Bioconda
##############

Executables provided by Conda/Bioconda can be installed and injected into the user's pipeline configuration, provided
the user has Miniconda installed and in PATH. Executables are defined by a list of ``CondaPackage`` tuples, with the
option to override the default conda channels that Operon loads.

.. code-block:: python

    from operon.components import CondaPackage

    def conda(self):
        return {
            'channels': ['overriding', 'channels', 'here'],
            'packages': [
                CondaPackage(tag='star=2.4.2a', config_key='STAR', executable_path='bin/STAR'),
                CondaPackage(tag='picard', config_key='picard', executable_path='share/picard-2.15.0-0/picard.jar')
            ]
        }

If provided, ``channels`` will be loaded by Miniconda in list order, which means the last entry has the highest
precedence, the second-highest entry has the second-highest precedence, etc.

A ``CondaPackage`` named tuple takes the following keys:

* ``tag`` is the name of the executable and optional version number fed directly to Miniconda
* ``config_key`` is the outermost key in the pipeline's ``configuration()``. When this executable is injected into
  a user's pipeline config, it's placed into ``pipeline_config[config_key]['path']``
* ``executable_path`` is only necessary if the basename of the installed executable is different from the conda tag, or
  if the developer wishes to use an executable outside conda's default ``bin`` folder. Some examples:

    * The conda package ``star=2.4.2a`` is installed as ``STAR``, so ``executable_path=`` must be set to ``bin/STAR``
    * The conda package ``picard`` installs an executable into ``bin``, but if the developer wishes to access the
      jar file directly, she must set ``executable_path=`` to ``share/picard-2.15.0-0/picard.jar``
    * The conda package ``bwa`` installs an executable into ``bin`` called ``bwa``, so ``executable_path`` does not
      need to be set

To see which executables are offered by Bioconda, please refer to their `package index
<https://bioconda.github.io/recipes.html>`_.

Parsl Configuration
###################

A default Parsl configuration can be provided in the event the user doesn't provide any higher-precendence Parsl
configuration. The returned ``dict`` will be fed directly to Parsl before execution.

.. code-block:: python

    def parsl_configuration(self):
        return {
            'sites': [
                {
                    'site': 'Local_Threads',
                    'auth': {'channel': None},
                    'execution': {
                        'executor': 'threads',
                        'provider': None,
                        'max_workers': 4
                    }
                }
            ],
            'globals': {'lazyErrors': True}
        }

To better understand Parsl configuration, please refer to `their documentation
<http://parsl.readthedocs.io/en/latest/userguide/configuring.html>`_ on the subject.

.. note::

    This method of configuring Parsl has very low precedence, and that's on purpose. The user is given every
    opportunity to provide a configuration that works for her specific platform, so the configuration provided
    by the pipeline is only meant as a desperation-style "we don't have anything else" configuration.

Pipeline Configuration
######################

Configuration values which may change from platform to platform, but won't change from run to run

Pipeline Arguments
##################

Arguments given on the command line; values which will change from run to run


Pipeline Logic
^^^^^^^^^^^^^^

Software ``operon.components.Software``
#######################################
``operon.components.Software``

A ``Software`` instance is an abstraction of an executable program external to the pipeline.

.. code-block:: python

    from operon.components import Software

    bwa = Software(name='bwa', path='/path/to/bwa')
    samtools = Software(name='samtools', path='/path/to/samtools')
    genecounter = Software(name='genecounter', path='/path/to/genecounter')

To register an Executable node in the workflow graph, call the ``Software`` instance's ``.register()`` method.
``register()`` takes any of ``Parameter``, ``Redirect``, ``Pipe``. Keyword arguments ``extra_inputs=`` and
``extra_outputs=`` can also be given to pass in respective lists of ``Data()`` input and output that isn't defined
as a command line argument to the Executable.

.. code-block:: python

    bwa.register(
        Parameter('--fastq', Data('/path/to/fastq')),
        Parameter('--phred', '33'),
        Redirect(stream=Redirect.STDERR, dest='/logs/bwa.log'),
        extra_inputs=[Data('/path/to/indexed_genome.fa')],
        extra_outputs=[Data('/path/to/bam')]
    )

CodeBlock
#########


Parameter
#########

Redirect
########

Pipe
####

ParslPipeline
#############
