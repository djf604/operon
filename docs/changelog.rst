Changelog
=========

v0.1.1 (released Jan 2018)
--------------------------
* If the ``path=`` argument isn't provided to a ``Software`` instance, the path will attempt to populate from
  ``pipeline_config[software_name]['path']``
* Added ``subprogram=`` argument to ``Software``
* Made tab completer program silent if it ever fails because it doesn't exist

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