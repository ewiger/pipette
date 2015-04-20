import os
import unittest
# We include <root>/lib/python and point to tests/mock/root/etc/config
# extend_path = lambda root_path, folder: sys.path.insert(
#     0, os.path.join(root_path, folder))
# ROOT = os.path.dirname(os.path.dirname(__file__))
# extend_path(ROOT, '')
# extend_path(ROOT, 'src')
# Import pipette
from pipette.pipes import Pipe, Process
from StringIO import StringIO


BASEDIR = os.path.dirname(__file__) or os.getcwd()


class FooProcess(Process):

    def run(self):
        self.results['foo'] = 'bar'
        #print self.results


class BrainyTest(unittest.TestCase):

    def test_description_parsing(self):
        pipe = Pipe(process_namespaces=['tests'])
        pipe.parse_definition_file(os.path.join(BASEDIR, 'foo.pipe'))
        output = StringIO()
        pipe.communicate(pipe_streams={
            'input': StringIO('{}'),
            'output': output,
        })
        process = pipe.chain.pop()
        expected_result = '{foo: bar}'
        assert process.streams['output'].getvalue().strip() == expected_result

    def test_no_type_in_description(self):
        pipe = Pipe(process_namespaces=['tests'])
        pipe.parse_definition_file(os.path.join(BASEDIR, 'no_type.pipe'))
        output = StringIO()
        error = StringIO()
        with self.assertRaises(AttributeError):
            pipe.communicate(pipe_streams={
                'input': StringIO('{}'),
                'output': output,
                'error': error,
            })
        # process = pipe.chain.pop()
        # expected_result = "'module' object has no attribute 'BashCommand'"
        # print process.streams['error'].getvalue().strip()
        # assert process.streams['error'].getvalue().strip() == expected_result
