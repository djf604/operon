======
Operon
======

.. image:: https://img.shields.io/pypi/v/nine.svg?style=flat-square
  :target: https://pypi.python.org/pypi/operon/
  :alt: Latest Version

.. image:: https://img.shields.io/pypi/status/Django.svg?style=flat-square
  :target: https://pypi.python.org/pypi/operon/
  :alt: Status

*Operon is a front-end abstraction to the powerful* `Parsl <http://parsl-project.org/>`_ *library, which supports the
execution of asynchronous and parallel data-oriented workflows.*

Operon provides an abstraction that allows developers to focus on the programs in their pipeline and the data flowing
in and out of those programs. Once the pipeline is written, it's portable enough to be run anywhere in a way that is
most efficient for the platform.

Operon also keeps track of pipeline installations and provides simple dependency management. That means the
time from installation to running a pipeline could be measured in minutes.

.. code-block:: text

    $ operon install /path/to/simple_pipeline.py
    $ operon configure simple_pipeline
    $ operon run simple_pipeline

For more information, please refer to the `documentation <http://operon.readthedocs.io/>`_.
