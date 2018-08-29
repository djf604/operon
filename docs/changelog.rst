Changelog
=========

v0.1.8 (released 29 August 2018)
--------------------------------
* Moved to Parsl 6.0+ exclusively, now only accepting class based Parsl configurations
* Integrated TinyDB for the framework state DB
* Tab autocompleter now automatically updates itself if needed
* Relaxed requirements on the name of the pipeline class, though ``Pipeline`` is still recommended
* Removed 'append' style redirects (``>>``) because Parsl doesn't support them at this time
* Stderr redirects are now properly respected on the lefthand side of a Pipe
* Supporting information added to run logs such as using the run name in the filename, logging the original command
  used to start the run, etc

v0.1.5 (released 7 June 2018)
-----------------------------
* Software execution itself can now be considered as input in the workflow graph
* Default dfk execution set down to 2 Python threads
* Fixed future state reporting
* Operon's temporary directory is now set to the same place as ``--logs-dir`` in hopes that this directory
  will exist on worker nodes which may not have access to the same filesystem as the head node
* ``Data('')`` now returns an empty string
* Added a ``batch-run`` execution mode, which allows for multiple concurrent runs of a single pipeline, each
  with different input and output, using the same resources pool
* Added support for multiple resources pools
* Added unit tests

v0.1.2 (released 21 Feb 2018)
-----------------------------
* Better error messages when the pipeline developer doesn't override a method properly
* Better error messages when the pipeline configuration is malformed
* Added ``parsl_config`` key to all pipeline configurations by default
* Added ``>`` and ``2>`` paths to the string representation of a Software's command
* Removed captured log output from screen; it still goes to the log file
* Added tags marking start time, end time, and duration of pipeline run to the log output
* Start and end time for each Software or CodeBlock added to log output
* Added "currently running" log entry, whenever the set of running programs changes
* Updated output for ``operon list``
* Updated output for ``operon show``
* When a conda environment already exists, added ability to reinstall
* Added ``operon uninstall`` to remove pipelines
* Refactored cleanup so that it always runs, even if some programs fail during the run

v0.1.1 (released Jan 2018)
--------------------------
* If the ``path=`` argument isn't provided to a ``Software`` instance, the path will attempt to populate from
  ``pipeline_config[software_name]['path']``
* Added ``subprogram=`` argument to ``Software``
* Made tab completer program silent if it ever fails because it doesn't exist

