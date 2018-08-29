Developer Tutorial
==================

This tutorial is meant to guide you, the pipeline developer, through all the necessary concepts to write an optimally
parallel and completely portable working pipeline in under 150 lines of code.

Since Operon was originally developed with bioinformatics in mind, we're going to be developing a bioinformatics
pipeline and using bioinformatics concepts, but Operon certainly isn't limited to any single field. The developer can
easily take the concepts talked about here and apply them to any set of software.

Let's get started. This tutorial assumes you have Operon installed. First we need a plain Python file; create one
called ``tutorial.py`` in whatever editor you desire. The very first thing we need to do is create a class called
``Pipeline`` which is a subclass of ``operon.components.ParslPipeline``. Add the following to ``tutorial.py``:

.. code-block:: python

    from operon.components import ParslPipeline


    class Pipeline(ParslPipeline):
        def description(self):
            pass

        def dependencies(self):
            pass

        def conda(self):
            pass

        def arguments(self, parser):
            pass

        def configuration(self):
            pass

        def pipeline(self, pipeline_args, pipeline_config):
            pass

Pipelines are defined by overriding methods in ``ParslPipeline`` as above. We'll fill in those methods now, going from
simple to more complex.

description(self)
-----------------
The ``description(self)`` method needs only to return a string that is used to describe the pipeline on various help
screens. Fill it in like so:

.. code-block:: python

    def description(self):
        return 'A tutorial pipeline'

dependencies(self)
------------------
The ``dependencies(self)`` method needs to return a list, where each element is a string representation of a Python
packages acceptable to ``pip``, which the user has the option to install at the time of pipeline installation.
Generally these packages should be ones that are used in the pipeline but aren't a part of the standard library. In
this tutorial, we're going to use ``PyVCF`` like so:

.. code-block:: python

    def dependencies(self):
        return ['pyvcf']

.. note::

    When the user installs Python dependencies, they're installed into the current Python environment, which may or
    may not be the correct environment for eventual pipeline execution. There isn't much the developer can do about
    this, so don't stress too much.

conda(self)
-----------
The ``conda(self`` method is used to define what external software the pipeline uses, and returns a dictionary with
two possible keys which are both optional:

.. code-block:: python

    {
        'packages': [
            # List of bioconda packages to install
        ],
        'channels': [
            # List of anaconda channels to use
        ]
    }

Unless ``channels`` is provided, Operon will use `bioconda <https://bioconda.github.io>`_ to download packages. The
elements of the ``packages`` key should be ``operon.components.CondaPackage`` objects (add that import now). We will
be using the following:

.. code-block:: python

    def conda(self):
        return {
            'packages': [
                CondaPackage(tag='bwa', config_key='bwa'),
                CondaPackage(tag='macs2=2.1.1', config_key='macs2'),
                CondaPackage(tag='samtools=1.6', config_key='samtools'),
                CondaPackage(tag='picard=2.9.2', config_key='picard'),
                CondaPackage(tag='freebayes=1.1.0', config_key='freebayes')
            ]
        }

.. note::

    The ``conda(self)`` method is not required but is a massive convenience to the user (which might also be you)
    because it enables the user to not have to track down and manually install software to run the pipeline. Everything
    defined here, which presumably encompasses all or most software used in the pipeline, can be automatically gathered
    and injected into the pipeline's configuration dictionary at the time of configuration.

arguments(self, parser)
-----------------------
For this simple tutoral, we'll only take three basic arguments like so:

.. code-block:: python

    def arguments(self, parser):
        parser.add_argument('--read', help='Path to read in fastq format')
        parser.add_argument('--output-dir', help='Path to an output directory')
        parser.add_argument('--lib', help='Name of this sample library')

configuration(self)
-------------------
Most of our configuration will be paths, which is a common practice, with a threading question thrown in. Notice in
``bwa|reference`` and ``freebayes|reference_fasta`` the expanded leaf type is used so that we can get those as ``path``
questions instead of plain ``text``, since we *are* asking for a path.

.. code-block:: python

    def configuration(self):
        return {
            'bwa': {
                'path': 'Path to bwa',
                'reference': {
                    'q_type': 'path',
                    'message': 'Path to a reference genome prefix for bwa'
                },
                'threads': 'Number of threads to run bwa'
            },
            'macs2': {
                'path': 'Path to macs2'
            },
            'samtools': {
                'path': 'Path to samtools'
            },
            'picard': {
                'path': 'Path to picard'
            },
            'freebayes': {
                'path': 'Path to freebayes',
                'reference_fasta': {
                    'q_type': 'path',
                    'message': 'Full path to reference fasta'
                }
            }
        }

pipeline(self, pipeline_args, pipeline_config)
----------------------------------------------
Now the main part of the pipeline building process. We've defined our periphery and can assume that the parameters
``pipeline_args`` and ``pipeline_config`` have been populated and that all the software we've asked for is installed.

Generally the first step is to define ``operon.components.Software`` instances:

.. code-block:: python

    def pipeline(self, pipeline_args, pipeline_config):
        freebayes = Software('freebayes')
        bwa_mem = Software('bwa', subprogram='mem')
        macs2 = Software('macs2', subprogram='callpeak')
        picard_markduplicates = Software(
            name='picard_markduplicates',
            path=pipeline_config['picard']['path'],
            subprogram='MarkDuplicates'
        )
        samtools_flagstat = Software(
            name='samtools_flagstat',
            path=pipeline_config['samtools']['path'],
            subprogram='flagstat'
        )
        samtools_sort = Software(
            name='samtools_sort',
            path=pipeline_config['samtools']['path'],
            subprogram='sort'
        )

For ``freebayes`` the instantiation is very simple: the default path resolution is fine, and there isn't a subprogram
to call. ``bwa mem`` and ``macs2 callpeak`` are slightly more involved, but only by adding a ``subprogram=`` keyword
argument.

For ``picard`` and ``samtools``, we're giving names that don't have a match in the configuration dictionary. That means
the default path resoluation won't work, so we need to give it paths explicitly with the ``path=`` keyword argument.

Next some very simple pipeline setup, just creating the output directory where the user defined output to go. There may
be more setup in more complicated pipelines. Add:

.. code-block:: python

    # Set up output directory
    os.makedirs(pipeline_args['output_dir'], exist_ok=True)

Now we can start constructing our pipeline workflow. Modify the import statments at the top of the file to:

.. code-block:: python

    import os
    from operon.components import ParslPipeline, CondaPackage, Software, Parameter, Redirect, Data, CodeBlock

We'll need all those components eventually.

The general idea the developer should take when constructing the pipeline workflow is to think of software as
stationary steps and data as elements flowing through those steps. Each stationary step has data coming in and data
going out. With that mental model, we can construct the following workflow:

.. code-block::

    bwa -> samtools sort -> picard markduplicates -> freebayes -> CodeBlock(pyvcf)
                                                  -> macs2
                         -> samtools flagstat

First we will run the software ``bwa`` whose output will flow into ``samtools sort``; this will be sequential so
there's no parallelization involved quite yet. The output of ``samtools sort`` will flow into both ``samtools flagstat``
and ``picard markduplicates``, forming our first two-way branch. These two program will run in parallel the moment that
``samtools sort`` produces its output. From there, the output of ``picard markduplicates`` flows as input into both
the ``freebayes`` variant caller and the ``macs2`` peak caller, forming another two-way branch. Finally, the output of
``freebayes`` will flow as input into a Python code block which uses ``PyVCF``. The overall workflow will terminate
when all leaves have completed; in this case, ``samtools flagstat``, ``macs2``, and the Python code block.

Let's dive in. The first software we need to insert into the workflow is ``bwa``:

.. code-block:: python

    alignment_sam_filepath = os.path.join(pipeline_args['output_dir'], pipeline_args['lib'] + '.sam')
    bwa_mem.register(
        Parameter('-t', pipeline_config['bwa']['threads']),
        Parameter(pipeline_config['bwa']['reference']),
        Parameter(Data(pipeline_args['read']).as_input()),
        Redirect(stream='>', dest=Data(alignment_sam_filepath).as_output(tmp=True))
    )

There's a lot going on here. First we define a filepath to send out alignment output from ``bwa``. Then we call the
``.register()`` method of ``bwa``, which signals to Operon that we want to insert ``bwa`` into the workflow as a
stationary step; the input data and output data flow is defined in the arguments to ``.register()``.

The first parameter is simple enough, just passing a ``-t`` in with the number of threads coming from our pipeline
configuration. The second parameter is a positional argument pointing to the reference genome we wish to use for
this alignment.

The third argument is importantly different. It's another positional argument mean to tell ``bwa`` where to find its
input fastq file, but the path is wrapped in a call to a ``Data`` object. Using a ``Data`` object is how Operon knows
which data/files on the filesystem should be considered as part of the workflow; failure to specify input or output
paths inside ``Data`` objects will cause Operon to miss them and may result in workflow programs running before they
have all their inputs! Notice also the ``.as_input()`` method is called on the ``Data`` object, which tells Operon not
only is this path important, but it should be treated as input data into ``bwa``.

Finally, the ``Redirect`` object (``bwa`` send its output to ``stdout``) sends the ``stdout`` stream to a filepath,
again wrapped in a ``Data`` object and marked as output. The ``tmp=True`` keyword argument tell Operon to delete
this file *after the whole pipeline is finished*, since we're not too interested in keeping that file around in our
final results.

.. code-block:: python

    sorted_sam_filepath = os.path.join(pipeline_args['output_dir'], pipeline_args['lib'] + '.sorted.sam')
    samtools_sort.register(
        Parameter('-o', Data(sorted_sam_filepath).as_output()),
        Parameter(Data(alignment_sam_filepath).as_input())
    )

The next step in the workflow is ``samtools sort``, which takes the output from ``bwa`` as input and produces some
output of its own. Notice again the important filepaths are wrapped in ``Data`` objects and given a specification as
input or output.

.. note::

    Although there is certainly a conceptual link between the output from ``bwa`` and the input here,
    that link does not need to be explicitly defined. As long as the same filepath is used, Operon will automatically
    recognize and link together input and output data flows between ``bwa`` and ``samtools sort``.

After ``samtools sort``, we're at our first intersection. It doesn't matter in which order we define the next steps, so
add something similar to:

.. code-block:: python

    samtools_flagstat.register(
        Parameter(Data(sorted_sam_filepath).as_input()),
        Redirect(stream='>', dest=sorted_sam_filepath + '.flagstat')
    )

    dup_sorted_sam_filepath = os.path.join(pipeline_args['output_dir'], pipeline_args['lib'] + '.dup.sorted.sam')
    picard_markduplicates.register(
        Parameter('I={}'.format(sorted_sam_filepath)),
        Parameter('O={}'.format(dup_sorted_sam_filepath)),
        Parameter('M={}'.format(os.path.join(pipeline_args['logs_dir'], 'marked_dup_metrics.txt'))),
        extra_inputs=[Data(sorted_sam_filepath)],
        extra_outputs=[Data(dup_sorted_sam_filepath)]
    )

There are a couple things to notice here. ``samtools flagstat`` takes input in a ``Data`` object but its output is a
plain string. This is because the output of ``samtools flagstat`` isn't used as an input into any other program, so
there's no need to treat it as a special file.

Notice also that both ``samtools flagstat`` and ``picard markduplicates`` use the same filepath as input; in fact that's
the point! Operon (really Parsl) will recognize that and make a parallel fork here, running each program at the same
time. As the developer, you didn't have to explicitly say to fork here, it is just what the workflow calls for. This is
what we mean by the workflow being automatically and optimally parallel.

Finally, there are some funny keyword arguments to ``picard_markduplicates.register()``. This is because of how the
parameters are passed to ``picard markduplicates``: it wants the form ``I=/some/path``, which mostly easily achieved
with a format string, as we've done. But if we were to pass in like this:

.. code-block:: python

    Parameter('I={}'.format(Data(sorted_sam_filepath).as_input()))

that wouldn't work. Why not? It's how we've passed in special input data in the other programs!

When the ``Data`` object is coerced into a string, it just becomes its path as a plain string. If a ``Data`` object is
used in a format string's ``.format()`` method, it will become it string representation before Operon can ever recognize
it. To mitigate that, we can explicitly tell Operon what the special input and output ``Data`` items are in the
``extra_inputs=`` and ``extra_outputs=`` keyword arguments, respectively. Notice in those keyword arguments we don't
need to call ``.as_input()`` or ``.as_output()`` because Operon can determine which is which from the keyword.

Now we come to our final two-way fork:

.. code-block:: python

    macs2_output_dir = os.path.join(pipeline_args['output_dir'], 'macs2')
    os.makedirs(macs2_output_dir, exist_ok=True)
    macs2.register(
        Parameter('--treatment', Data(dup_sorted_sam_filepath).as_input()),
        Parameter('--name', pipeline_args['lib']),
        Parameter('--outdir', macs2_output_dir),
        Parameter('--call-summits'),
        Parameter('--shift', '100')
    )

    vcf_output_path = os.path.join(pipeline_args['output_dir'], pipeline_args['lib'] + '.vcf')
    freebayes.register(
        Parameter('--fasta-reference', pipeline_config['freebayes']['reference_fasta']),
        Parameter(Data(dup_sorted_sam_filepath).as_input()),
        Redirect(stream='>', dest=Data(vcf_output_path).as_output())
    )

There isn't much going on here that we haven't already discussed, so we won't go into detail. Both ``macs2`` and
``freebayes`` use the output of ``picard markduplicates``, so they will run once the output of ``picard markduplicates``
is available.

Finally, the output of ``freebayes`` is used as input into a Python codeblock:

.. code-block:: python

    def get_first_five_positions(vcf_filepath, output_filepath):
        import vcf
        vcf_reader = vcf.Reader(open(vcf_filepath))
        with open(output_filepath, 'w') as output_file:
            for record in vcf_reader[:5]:
                output_file.write(record.POS + '\n')
    CodeBlock.register(
        func=get_first_five_positions,
        kwargs={
            'vcf_filepath': vcf_output_path,
            'output_filepath': os.path.join(pipeline_args['output_dir'], 'first_five.txt')
        },
        inputs=[Data(vcf_output_path)]
    )

``CodeBlocks`` can be thought of as very similar to ``Software`` instances with the same data flow model.


Running the Tutorial Pipeline
-----------------------------
The pipeline in complete! If you with to attempt to run it, simply install it into Operon, configure it (you might need
to create a reference genome index), and run it with the small data bundle linked below.

Download the tutorial `data bundle <https://drive.google.com/uc?export=download&id=1isOgCOuJ8U8dloWzeYJKtX3CA3sW7wgB>`_.

An awkward step in the Operon installation process is the necessity of a reference genome index. To generate one we
need a reference genome ``fasta`` file and then to run ``bwa index`` over it; this mean that when we first configure our
pipeline when it asks for the ``bwa`` reference genome prefix we won't have it, but we can come back and fill it in
later.

.. code-block:: bash

    $ /path/to/bwa index /path/to/downloaded/dm6.fa.gz

``bwa`` dumps its index in the same folder as the genome fasta, so the reference prefix is just the path to the
reference fasta.

Use the inluded *Drosophila melanogaster* DNA fastq as your ``--read`` input.

``freebayes`` can't deal with a compressed reference genome fasta, so uncompress it before passing the path in the
pipeline config.
