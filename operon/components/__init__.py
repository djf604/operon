import subprocess
import time
import json
import sys
import os

from pathos.multiprocessing import ProcessPool
import six


EXIT_ERROR = 1


class ParallelBlock(object):
    _run_method_map = {
        'multiprocess': '_run_multiprocess'
    }

    def __init__(self, method='multiprocess', block=True, processes=None, autorun=True):
        self.method = method
        self.should_block = block
        self.processes = processes
        self.autorun = bool(autorun)
        self.has_run = False

        self.software = list()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if not self.has_run and self.autorun:
            self.run()

    def add(self, *args):
        self.software.extend(args)

    def run(self):
        getattr(self, self._run_method_map.get(self.method, 'multiprocess'))()
        self.has_run = True

    def _run_multiprocess(self):
        pool = ProcessPool(nodes=self.processes)
        sentinel = None
        for software_prep in self.software:
            sentinel = pool.apipe(software_prep.run)
        sentinel.wait()


class Software(object):
    """
    The Software object is the main unit in command execution. It is instantiated with a name and
    a path, and commands are executed primarily with the run() method, which takes an arbitrary
    number of Parameter, Redirect, and Pipe objects. Software also takes a single keyword argument,
    shell, which when True will run the command directly on the shell as a string.

    cmd(self, *args) removed in version 0.2.5
    """
    def __init__(self, software_name, software_path):
        self.software_name = software_name
        self.software_path = software_path

    def run(self, *args, **kwargs):
        """
        Run this program with Parameters, Redirects, and Pipes. Prepares and immediately runs the
        returned SoftwareBlueprint() objectIf shell=True, this command is executed as a string
        directly on the shell; otherwise, it's executed using Popen processes and appropriate streams.
        :param args: 0 or more of Parameter, Redirect, and Pipe
        :param kwargs: shell=Bool
        """
        self.prep(*args, **kwargs).run()

    def pipe(self, *args, **kwargs):
        """
        This is a deprecated method, use prep() instead
        """
        return self.prep(*args, **kwargs)

    def prep(self, *args, **kwargs):
        """
        Prepares and returns a SoftwareBlueprint() object based on the Software() object
        :param args: 0 or more of Parameter, Redirect, and Pipe
        :param kwargs: shell=Bool
        :return: SoftwareBlueprint The blueprint object
        """
        return SoftwareBlueprint(self.software_path, *args, **kwargs)


class SoftwareBlueprint(object):
    """
    The SoftwareBlueprint object is responsible for actually assembling and executing programs and
    parameters.
    """
    def __init__(self, software_path, *args, **kwargs):
        self.shell = kwargs.get('shell', False)
        self.blueprint = SoftwareBlueprint._generate_blueprint(software_path, *args)
        # self.software_path = software_path

    def run(self):
        # Output log info for this command
        software_name = 'TODO'
        log_header = 'Running {}\n{}\n'.format(software_name, str(self))
        if _Settings.logger._is_active():
            _Settings.logger._write(log_header)
        else:
            sys.stdout.write(log_header)

        # TODO Pre-run callback hook
        if self.shell:
            subprocess.call(str(self), shell=True, executable=os.environ['SHELL'])
        else:
            output_stream_filehandles = list()
            blueprint_processes = list()

            # For each command in the blueprint, set up streams and Popen object to execute
            for i, cmd in enumerate(self.blueprint):
                stdin_stream = None if i == 0 else blueprint_processes[i - 1].stdout
                stdout_filehandle = None
                stderr_filehandle = None

                # If this command isn't the last in the list, that means the output
                # is being piped into the next command
                if i + 1 < len(self.blueprint):
                    stdout_filehandle = subprocess.PIPE
                # If this is the last command in the list, stdout may be redirected to a file...
                elif cmd['stdout']:
                    redir = cmd['stdout']
                    stdout_filehandle = open(redir.dest, redir.mode)
                    output_stream_filehandles.append(stdout_filehandle)
                # ...or it may be set out to the main log file
                elif _Settings.logger.log_stdout and _Settings.logger.destination:
                    stdout_filehandle = subprocess.PIPE

                # stderr can be redirected regardless of piping
                if cmd['stderr']:
                    redir = cmd['stderr']
                    stderr_filehandle = open(redir.dest, redir.mode)
                    output_stream_filehandles.append(stderr_filehandle)
                # Or it may be sent out to a log file
                elif (
                        _Settings.logger.log_stderr
                        and (_Settings.logger.destination_stderr or _Settings.logger.destination)
                ):
                    stderr_filehandle = subprocess.PIPE

                # Create this process as a Popen object, with appropriate streams
                process = subprocess.Popen(cmd['cmd'], stdin=stdin_stream,
                                 stdout=stdout_filehandle, stderr=stderr_filehandle)
                blueprint_processes.append(process)

                # If this is the last command in the list, wait for it to finish
                if i + 1 == len(self.blueprint):
                    process.wait()

                    # If logging is set, capture stdout (or stderr) to log file
                    # TODO I think the logic here can be expressed more concisely
                    if _Settings.logger.log_stdout and _Settings.logger.destination and process.stdout:
                        for line in process.stdout:
                            _Settings.logger._write(line)
                    if (
                            _Settings.logger.log_stderr
                            and (_Settings.logger.destination_stderr or _Settings.logger.destination)
                            and process.stderr
                    ):
                        for line in process.stderr:
                            _Settings.logger._write(line, bool(_Settings.logger.destination_stderr))

            # Close all the file handles created for redirects
            map(lambda f: f.close(), output_stream_filehandles)

    @staticmethod
    def _generate_blueprint(software_path, *args):
        # shell = kwargs.get('shell', False)
        # If shell=True, return a full command string
        # if shell:
        #     return '{software_path}{parameters}'.format(
        #         software_path=software_path,
        #         parameters=' '.join([''] + [str(p) for p in args])
        #     )

        # If shell=False, we have to get much fancier
        cmd_parts = {
            'Parameter': [para for para in args if isinstance(para, Parameter)],
            'Redirect': [redir for redir in args if isinstance(redir, Redirect)],
            'Pipe': [pipe for pipe in args if isinstance(pipe, Pipe)]
        }

        # If there is more than 2 redirects or 1 pipe, ignore extras
        if len(cmd_parts['Redirect']) > 2:
            cmd_parts['Redirect'] = cmd_parts['Redirect'][:2]
        if len(cmd_parts['Pipe']) > 1:
            cmd_parts['Pipe'] = cmd_parts['Pipe'][:1]

        # Set software path and parameters list
        cmd = software_path.split()
        for para in cmd_parts['Parameter']:
            cmd += para.parameters

        # Set appropriate Redirect objects
        stdout, stderr = None, None
        for redir in cmd_parts['Redirect']:
            if redir.stream in Redirect._STDOUT_MODES:
                stdout = redir
            elif redir.stream in Redirect._STDERR_MODES:
                stderr = redir
            elif redir.stream in Redirect._BOTH_MODES:
                stdout, stderr = redir, redir

        # Add this software command to the blueprint
        blueprint = [{
            'cmd': cmd,
            'stdout': stdout,
            'stderr': stderr
        }]

        # Recurse if there is a Pipe
        if cmd_parts['Pipe']:
            pipe = cmd_parts['Pipe'][0]
            blueprint.extend(pipe.piped_software_blueprint.blueprint)

        return blueprint

    def __unicode__(self):
        return self.__str__()

    def __str__(self):
        return ' | '.join([
            '{cmd}{redir_out}{redir_err}'.format(
                cmd=' '.join(cmd['cmd']),
                redir_out='' if not isinstance(cmd['stdout'], Redirect) else ' ' + str(cmd['stdout']),
                redir_err='' if not isinstance(cmd['stderr'], Redirect) else ' ' + str(cmd['stderr'])
            )
            for cmd in self.blueprint
        ])


class Parameter(object):
    """
    The Parameter object abstracts out passing parameters into a Software object.
    """
    def __init__(self, *args):
        self.parameters = [
            split_arg
            for arg in args
            for split_arg in str(arg).split()
        ]

    def __str__(self):
        return ' '.join(self.parameters)


class Redirect(object):
    """
    The Redirect object abstracts out redirecting streams to files.
    """
    STDOUT = 0
    STDERR = 1
    BOTH = 2
    STDOUT_APPEND = 3
    STDERR_APPEND = 4
    BOTH_APPEND = 5
    NULL = os.devnull
    _APPEND_MODES = {STDOUT_APPEND, STDERR_APPEND, BOTH_APPEND}
    _STDOUT_MODES = {STDOUT, STDOUT_APPEND}
    _STDERR_MODES = {STDERR, STDERR_APPEND}
    _BOTH_MODES = {BOTH, BOTH_APPEND}

    _convert = {
        '>': STDOUT,
        '1>': STDOUT,
        '>>': STDOUT_APPEND,
        '1>>': STDOUT_APPEND,
        '2>': STDERR,
        '2>>': STDERR_APPEND,
        '&>': BOTH,
        '&>>': BOTH_APPEND
    }

    def __init__(self, stream=STDOUT, dest='out.txt'):
        if isinstance(stream, six.string_types):
            stream = Redirect.token_convert(str(stream).strip())

        self.stream = stream
        self.dest = dest
        self.mode = 'a' if stream in Redirect._APPEND_MODES else 'w'

    def __str__(self):
        return ''.join([Redirect.token_convert(self.stream), str(self.dest)])

    @staticmethod
    def token_convert(token):
        if type(token) == str:
            return Redirect._convert[token]
        elif type(token) == int:
            reverse_convert = {v: k for k, v in six.iteritems(Redirect._convert)}
            return reverse_convert[token]
        return Redirect.STDOUT


class Pipe(object):
    def __init__(self, piped_software_blueprint):
        self.piped_software_blueprint = piped_software_blueprint

    def __str__(self):
        return str(self.piped_software_blueprint)


class _Settings(object):
    class _Logger:
        def __init__(self):
            """
            Initialize default logging settings. If destination is not None, the logger is 
            considered active and will capture all output stream content from pipeline 
            programs that isn't redirected explicitly by the user.
            
            If destination_stderr is not None, any capture stderr output will be shuttled 
            into it's own file, the value of destination_stderr. Otherwise, it'll be mixed 
            in with the stdout file.
            
            If either log_stdout or log_stderr are false, that particular output stream will 
            be left alone, and probably sent to the screen.
            """
            self.destination = None
            self.destination_mode = 'w'
            self.destination_stderr = None
            self.destination_stderr_mode = None

            self.log_stdout = True
            self.log_stderr = True

            self._dest_filehandle = None
            self._dest_stderr_filehandle = None

        def set(self, **kwargs):
            """
            User calls this method to set logger settings based on keywords. When the user 
            changes destination or destination_stderr, operations are performed to open 
            up a filehandle and make it available globally.
            
            If an exception occurs while trying to open either of the log files, it will 
            fail silently. allowing that output stream to go to the screen as a default.
            
            TODO Prevent from creating an empty stderr.log file
            :param kwargs: dict Logger settings to set
            """
            for kw in ('destination_mode', 'destination_stderr_mode'):
                # Set file mode flags
                if kw in kwargs:
                    setattr(self, kw, str(kwargs[kw]))

            for kw in ('log_stdout', 'log_stderr'):
                # Set whether to log for either output stream
                if kw in kwargs:
                    setattr(self, kw, bool(kwargs[kw]))

            if 'destination' in kwargs and kwargs['destination']:
                # Set default destination path and open file
                try:
                    self.destination = str(kwargs['destination'])

                    # If directory to the log doesn't exist, create it
                    logdir = os.path.dirname(self.destination)
                    if logdir and not os.path.exists(logdir):
                        os.makedirs(logdir, mode=0o755)

                    # Open destination filehandle
                    if self._dest_filehandle is not None:
                        self._dest_filehandle.close()
                    self._dest_filehandle = open(self.destination, self.destination_mode)
                except Exception as e:
                    # If anything goes wrong, reset everything
                    self.destination = None
                    sys.stderr.write('Error opening log file for stdout\n{}\n'.format(e.message))

            if 'destination_stderr' in kwargs and kwargs['destination_stderr']:
                try:
                    self.destination_stderr = str(kwargs['destination_stderr'])

                    # If directory to the stderr log doesn't exist, create it
                    logdir_stderr = os.path.dirname(self.destination_stderr)
                    if logdir_stderr and not os.path.exists(logdir_stderr):
                        os.makedirs(logdir_stderr, mode=0o755)

                    # Open destination_stderr filehandle
                    stderr_mode = (self.destination_stderr_mode
                                   if self.destination_stderr_mode
                                   else self.destination_mode)
                    if self._dest_stderr_filehandle is not None:
                        self._dest_stderr_filehandle.close()
                    self._dest_stderr_filehandle = open(self.destination_stderr, stderr_mode)
                except Exception as e:
                    # If anything goes wrong, reset everything
                    self.destination_stderr = None
                    sys.stderr.write('Error opening log file for stderr\n{}\n'.format(e.message))

        def _is_active(self):
            return bool(self.destination)

        def _write(self, s, to_stderr_log=False, timestamp=True):
            """
            Writes out string s to a given filehandle, with optional timestamp.
            This method is used to capture all output streams from various pipeline programs 
            that aren't explicitly redirected into a single log file.
            :param s: str Captured output stream content
            :param to_stderr_log: bool Whether to write contents to separate stderr file
            :param timestamp: bool Whether to prepend a timestamp to every line of output
            """
            if timestamp:
                line = '{}>\t{}'.format(time.strftime('%d%b%Y %H:%M:%S'), s)
            else:
                line = '\t' + s

            logfh = self._dest_stderr_filehandle if to_stderr_log else self._dest_filehandle
            if logfh is not None:
                logfh.write(line)

        def _close_all(self):
            """
            Closes all open filehandles. Also Nones the file destinations.
            """
            if self._dest_filehandle is not None:
                self._dest_filehandle.close()
                self._dest_filehandle = None
                self.destination = None
            if self._dest_stderr_filehandle is not None:
                self._dest_stderr_filehandle.close()
                self._dest_stderr_filehandle = None
                self.destination_stderr = None

    logger = _Logger()


class BasePipeline(object):
    """
    The BasePipeline object is meant to be an abstract class for a Pipeline class. This
    class gives a method for parsing the config file and sets up some necessary
    class variables.
    """
    pipeline_args = None
    pipeline_config = None
    settings = _Settings()

    def description(self):
        """
        Override this method.
        A single string describing this pipeline.
        :return: str A description of this pipeline.
        """
        return ''

    def dependencies(self):
        """
        Override this method.
        A list of pip style dependencies for this pipeline.
        :return: list A list of pip style dependencies.
        """
        return list()

    def conda(self):
        """
        Override this method.
        Directives to download software from conda/bioconda.

        The returned dictionary should have a key 'packages' and an
        optional key 'channels'.
            - 'packages' should be a list of operon.components.CondaPackage tuples
            - 'channels' should be list of conda channels to temporarily access for instllation

        :return: dict A dict configuring conda packages
        """
        return dict()

    def add_pipeline_args(self, parser):
        """
        Override this method.
        Adds arguments to this pipeline using the argparse.add_argument() method. The parser
        argument is an argparse.ArgumentParser() object.
        :param parser: argparse.ArgumentParser object
        """
        pass

    def configure(self):
        """
        Override this method.
        Dictionary representation of the JSON object that will be used to configure this
        pipeline. Configuration variables should be values that will change from platform
        to platform but not run to run, ex. paths to software, but likely not parameters to
        that software.

        Keys will be returned as is, but terminal string values will become input values from
        the user when he/she calls operon configure.

        In the pipeline, this dictionary will become the class variable pipeline_config, with
        terminal string values replaced with user input values.
        :return: dict Dictionary representation of config values
        """
        return dict()

    def run_pipeline(self, pipeline_args, pipeline_config):
        """
        Override this method.
        The logic of the pipeline will be here. The arguments are automatically populated with the
        user arguments to the pipeline (those added with the method add_pipeline_args()) as well as
        the configuration for the pipeline (as a dictionary of the form returned by the method config(),
        but with user input values in place of terminal strings.)
        :param pipeline_args: dict Populated dictionary of user arguments
        :param pipeline_config: dict Populated dictionary of pipeline configuration
        :return: None
        """
        raise NotImplementedError

    def _run_pipeline(self, pipeline_args, pipeline_config):
        self.run_pipeline(pipeline_args=pipeline_args, pipeline_config=pipeline_config)
        self.settings.logger._close_all()

    def _print_dependencies(self):
        sys.stdout.write('\n'.join(self.dependencies()) + '\n')

    def _parse_config(self):
        try:
            with open(self.pipeline_args['config']) as config:
                self.pipeline_config = json.loads(config.read())
        except IOError:
            sys.stdout.write('Fatal Error: Config file at {} does not exist.\n'.format(
                self.pipeline_args['config']
            ))
            sys.stdout.write('A config file location can be specified with the --config option.\n')
            sys.exit(EXIT_ERROR)
        except ValueError:
            sys.stdout.write('Fatal Error: Config file at {} is not in JSON format.\n'.format(
                self.pipeline_args['config']
            ))
            sys.exit(EXIT_ERROR)
