'''
Contains pipes, process and some examples.

Copyright (c) 2014 Yauhen Yakimovich
Licensed under the MIT License (MIT). Read a copy of LICENSE distributed with
this code.
'''

from abc import ABCMeta, abstractmethod
import os
import sys
from StringIO import StringIO
from subprocess import Popen

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


class Process(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.description = dict()
        self.streams = {
            'input': sys.stdin,
            'output': sys.stdout,
            'error': sys.stderr,
        }
        self.parameters = dict()
        self.results = dict()

    def is_filepath_safe(self, path):
        return os.path.exists(path)

    def parse_input(self):
        '''Parse program parameters from input stream'''
        input_stream = self.streams['input']
        input_yaml = ''
        if isinstance(input_stream, basestring):
            input_yaml = input_stream
        elif hasattr(input_stream, 'read'):
            input_yaml = input_stream.read()
        else:
            raise IOError('Can not read from input stream')
        input_parameters = yaml.load(input_yaml, Loader=Loader)
        self.parameters.update(input_parameters)

    def print_line(self, line, stream_name='output', append_newline=True):
        if append_newline:
            line += '\n'
        self.streams[stream_name].write(line)

    def flush_streams(self):
        '''Flush output streams'''
        self.streams['output'].flush()
        self.streams['error'].flush()

    def bake_output(self):
        '''Prepare and stream output'''
        return yaml.dump(self.results)

    def execute(self, default_parameters={}):
        self.parameters.update(default_parameters)
        self.put_on()
        self.run()
        self.reduce()

    def put_on(self):
        '''Induce execution. Happens before run(). Can be job split.'''
        # Input
        self.parse_input()

    @abstractmethod
    def run(self):
        '''
        This method does actual work using self.parameters and saving results
        into self.results.
        '''
        raise NotImplemented("Called abstract method `Process.run`")

    def reduce(self):
        '''Reduce execution. Happens after run(). Can be results join.'''
        # Output
        output = self.bake_output()
        self.streams['output'].write(output)
        self.flush_streams()


class BashCommand(Process):

    def run(self):
        # Get parameters.
        bash_command = self.parameters.get('bash_command')

        if 'input_filepath' in self.parameters:
            file_path = self.parameters['input_filepath']
            assert self.is_filepath_safe(file_path)
            command_input = open(file_path)
        else:
            command_input = None

        if 'output_filepath' in self.parameters:
            file_path = self.parameters['output_filepath']
            assert self.is_filepath_safe(file_path)
            command_output = open(file_path, 'w+')
        else:
            command_output = None

        if 'error_filepath' in self.parameters:
            file_path = self.parameters['error_filepath']
            assert self.is_filepath_safe(file_path)
            command_error = open(file_path, 'w+')
        else:
            command_error = None

        # Call the subprocess.
        subprocess = Popen(
            bash_command,
            stdin=command_input,
            stdout=command_output,
            stderr=command_error,
            shell=True,
            executable='/bin/bash',
        )
        subprocess.communicate()


class Pipe(object):
    '''
    A trivial chain of processes defined by YAML.

    The pipeline is defined by a single YAML file, which is read by
    :meth:`parse_definition_file` *or* passed to the constructor with
    the `definition` parameter.

    The definition should contain a ``chain:`` list attribute, which
    is then interpreted as the list of processes to run.  Each item in
    the ``chain:`` list must be a valid data structure defining a
    `Process`:class: (which see).
    '''

    def __init__(self, process_namespaces=['pipette'], definition=None):
        self.process_namespaces = process_namespaces
        self.definition = definition
        self.chain = None

    @property
    def name(self):
        return self.definition['name']

    @property
    def pipe_extension(self):
        return '.pipe'

    def parse_definition_file(self, definition_filepath):
        """
        Read pipeline definition from the given file.

        The pipeline name is set to the name of the file, stripped of
        the extension.

        **Note:**

        - Any existing definition is *overwritten*.
        - The file name *must* end with the string returned by
          `Pipe.pipe_extension`.
        """
        # Get pipe name.
        pipe_filename = os.path.basename(definition_filepath)
        if not pipe_filename.endswith(self.pipe_extension):
            raise Exception('Wrong pipe description filename: %s' %
                            pipe_filename)
        # remove extension
        pipe_name = pipe_filename[:-len(self.pipe_extension)]
        # Parse stream.
        with open(definition_filepath) as definition_stream:
            try:
                self._parse_definition(pipe_name, definition_stream)
            except yaml.parser.ParserError as error:
                raise IOError("File '%s' cannot be parsed as a YAML stream: %s"
                              % (definition_filepath, error))

    def _parse_definition(self, pipe_name, stream):
        self.definition = {'name': pipe_name}
        self.definition.update(yaml.load(stream, Loader=Loader))

    def _instanciate_processes(self):
        '''Iterator that bakes processes'''
        for process_description in self.definition['chain']:
            yield self._instanciate_process(process_description)

    def _instanciate_process(self, process_description,
                            default_type='BashCommand'):
        cls = self.find_process_class(
            process_description.get('type', default_type))
        process = cls()
        assert isinstance(process, Process)
        # record the description used to build this
        process.description.update(process_description)
        # add default params
        default_parameters = process_description.get(
            'default_parameters', {})
        process.parameters.update(default_parameters)
        # ensure the process has a name
        default_process_name = process.__class__.__name__.lower()
        process.parameters['name'] = process_description.get(
            'name', default_process_name)
        return process

    def find_process_class(self, process_type):
        module = None
        for process_namespace in self.process_namespaces:
            process_type = process_namespace + '.' + process_type
            module_name, class_name = process_type.rsplit('.', 1)
            try:
                module = __import__(module_name, {}, {}, [class_name])
            except ImportError:
                pass
        if module is None:
            raise ImportError('Failed to find/import process type: %s in %s' %
                              (process_type, self.process_namespaces))
        return getattr(module, class_name)

    @staticmethod
    def _get_default_streams(streams):
        result = {}
        for name, default in (
                ('input',  sys.stdin),
                ('output', sys.stdout),
                ('error',  sys.stderr),
        ):
            result[name] = streams.get(name, default)
        return result

    def communicate(self, pipe_streams={}):
        '''
        Run processes in the pipeline in a sequence.

        Optional argument `pipe_streams` allows setting the input,
        output and error streams for the whole pipeline.  Note that
        only the ``error`` stream setting is shared by all `Process`
        instances in the pipeline: ``input`` and ``output`` settings
        are used only for the first (resp. last) process in the
        pipeline.
        '''
        pipe_streams = self._get_default_streams(pipe_streams)
        self.chain = list(self._instanciate_processes())
        chain_size = len(self.chain)
        assert chain_size > 0
        output_stream = StringIO()
        input_stream = None
        for index, process in enumerate(self.chain):
            # Prepare stream wiring.
            process.streams['error'] = pipe_streams['error']
            if index == 0:
                # First process.
                process.streams['input'] = pipe_streams['input']
            else:
                # In-between process.
                process.streams['input'] = input_stream

            if index == chain_size - 1:
                # Last process.
                process.streams['output'] = pipe_streams['output']
            else:
                # In-between process.
                process.streams['output'] = output_stream

            # Do actual work. Take pipe-wide parameters that will override
            # individual defaults.
            pipe_parameters = self.definition.get('pipe_parameters', {})
            self.execute_process(process, pipe_parameters)

            # Maintain stream wiring.
            input_stream = output_stream
            try:
                # The code assumes that `input_stream` (== former
                # `output_stream`) is a seekable stream (e.g.,
                # StringIO) but this fails when the chain consists of
                # a single process.  In this case, however, we can
                # ignore if the seek operation fails.
                input_stream.seek(0)
            except IOError as err:
                # errno 29 is "Illegal seek"
                if err.errno == 29 and chain_size == 1:
                    pass
                else:
                    raise
            output_stream = StringIO()

    def execute_process(self, process, parameters):
        '''
        Can be extended with custom behavior, surrounding the actual process
        executions with before and after logic in an aspect-oriented way.
        '''
        process.execute(parameters)
