import os
import sys
import unittest
# We include <root>/lib/python and point to tests/mock/root/etc/config
extend_path = lambda root_path, folder: sys.path.insert(
    0, os.path.join(root_path, folder))
ROOT = os.path.dirname(os.path.dirname(__file__))
extend_path(ROOT, '')
extend_path(ROOT, 'src')
# Import pipette
from pipette import Pipe, Process
from StringIO import StringIO


class FooProcess(Process):

    def run(self):
        self.results['foo'] = 'bar'
        #print self.results


FOO = '''
name: 'foo_pipe'

chain:
    - type: 'test_workflow.FooProcess'
      foo_value: 'baz'

'''


class BrainyTest(unittest.TestCase):

    def test_description_parsing(self):
        pipe = Pipe(process_namespace='tests')
        pipe.parse_definition(pipe_name='foo', stream=StringIO(FOO))
        output = StringIO()
        pipe.communicate(pipe_streams={
            'input': StringIO('{}'),
            'output': output,
        })
        process = pipe.chain.pop()
        expected_result = '{foo: bar}'
        assert process.streams['output'].getvalue().strip() == expected_result
