'''
pipette
=======

A library implementing a protocol to simplify programming pipeline-like
chains of intercommunicating processes.

Copyright (c) 2014 Yauhen Yakimovich
Licensed under the MIT License (MIT). Read a copy of LICENSE distributed with
this code.
'''
import os
import sys
import json
from StringIO import StringIO
from subprocess import Popen
from minify_json import json_minify


__version__ = '0.1.1'


def get_default_streams(streams):
    if not 'input' in streams:
        streams['input'] = sys.stdin
    if not 'output' in streams:
        streams['output'] = sys.stdout
    if not 'error' in streams:
        streams['error'] = sys.stderr
    return streams


class Process(object):

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
        json_data = self.streams['input'].read()
        json_data = json_minify(json_data, strip_space=False)
        if not json_data:
            return
        input_parameters = json.loads(json_data)
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
        return json.dumps(self.results)

    def execute(self, default_parameters={}):
        self.parameters.update(default_parameters)
        self.put_on()
        self.run()
        self.reduce()

    def put_on(self):
        '''Induce execution. Happens before run(). Can be job split.'''
        # Input
        self.parse_input()

    def run(self):
        '''
        This method does actual work using self.parameters and saving results
        into self.results.
        '''

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
            command_input = StringIO(self.parameters.get('command_input', ''))

        if 'output_filepath' in self.parameters:
            file_path = self.parameters['output_filepath']
            assert self.is_filepath_safe(file_path)
            command_output = open(file_path, 'w+')
        else:
            command_output = StringIO()

        if 'error_filepath' in self.parameters:
            file_path = self.parameters['error_filepath']
            assert self.is_filepath_safe(file_path)
            command_error = open(file_path, 'w+')
        else:
            command_error = StringIO()
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
        # Collect results.
        self.results.update(self.parameters)
        if not 'output_filepath' in self.results:
            self.results['output'] = command_output.get_value()
        if not 'error_filepath' in self.results:
            self.results['error'] = command_error.get_value()


class Pipe(object):
    '''
    A trivial chain of processes defined by JSON.
    '''

    def __init__(self, definition):
        self.process_namespace = 'pippete'
        self.definition = definition
        self.chain = None

    @property
    def name(self):
        return self.definition['name']

    @classmethod
    def parse_definition(self, definition_filepath):
        with open(definition_filepath) as definition_file:
            try:
                # skip comments
                json_data = json_minify(definition_file.read(),
                                        strip_space=False)
                definition = json.loads(json_data)
            except ValueError as error:
                raise IOError('JSON parsing error in: "%s".\n%s' %
                              (definition_filepath, error.message))
        # Get pipe name.
        pipe_filename = os.path.basename(definition_filepath)
        if not pipe_filename.endswith('Pipe.json'):
            raise Exception('Wrong pipe description filename: %s' %
                            pipe_filename)
        definition['name'] = pipe_filename.replace('Pipe.json', '')
        return definition

    def bake_processes(self):
        '''Iterator that bakes processes'''
        for process_description in self.definition['chain']:
            process = self.instantiate_process(process_description)
            default_parameters = process_description.get(
                'default_parameters', {})
            process.parameters.update(default_parameters)
            yield process

    def find_process_class(self, process_type):
        process_type = self.process_namespace + '.' + process_type
        module_name, class_name = process_type.rsplit('.', 1)
        module = __import__(module_name, {}, {}, [class_name])
        return getattr(module, class_name)

    def instantiate_process(self, process_description,
                            default_type='BashCommand'):
        cls = self.find_process_class(
            process_description.get('type', default_type))
        process = cls()
        assert isinstance(process, Process)
        default_process_name = process.__class__.__name__.lower()
        process.parameters['name'] = process_description.get(
            'name', default_process_name,
        )
        process.description.update(process_description)
        return process

    def communicate(self, pipe_streams={}):
        pipe_streams = get_default_streams(pipe_streams)
        self.chain = list(self.bake_processes())
        chain_size = len(self.chain)
        assert chain_size > 0
        output_stream = StringIO()
        input_stream = None
        for index, process in enumerate(self.chain):
            # Prepare stream wiring.
            process.streams['error'] = pipe_streams['error']
            if chain_size == 1:
                # Single process.
                process.streams['input'] = pipe_streams['input']
                process.streams['output'] = pipe_streams['output']
            elif index == 0:
                # First process.
                process.streams['input'] = pipe_streams['input']
            else:
                # In-between process.
                process.streams['input'] = input_stream

            if index == chain_size:
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
            output_stream = StringIO()

    def execute_process(self, process, parameters):
        '''
        Can be extended with custom behavior, surrounding the actual process
        executions with before and after logic in an aspect-oriented way.
        '''
        process.execute(parameters)
