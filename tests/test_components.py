import os
import pytest
import tempfile
from operon.components import CondaPackage, Software, Data, Parameter, Redirect, Pipe, CodeBlock, ParslPipeline
from operon._util.apps import _ParslAppBlueprint, _DeferredApp


def test_data():
    # Reset _data storage
    Data._data = dict()

    # Turn into string as expected
    d_norm = Data('/path/to/data')
    assert str(d_norm) == '/path/to/data'

    # Multiple instantiations of same path return same instance
    d_same1 = Data('/path/to/same')
    d_same2 = Data('/path/to/same')
    assert str(d_same1) == str(d_same2)
    assert d_same1 is d_same2

    # Data class should have two stored paths
    assert len(Data._data) == 2

    # Mark Data object as input/output
    assert d_norm.mode is None
    d_norm_input = Data('/path/to/data').as_input()
    assert d_norm_input.mode == Data.INPUT
    d_norm_output = Data('/path/to/data').as_output()
    assert d_norm_output.mode == Data.OUTPUT

    # d_norm should also be output now, since paths were the same
    assert d_norm.mode == Data.OUTPUT

    # Data class should still only have two stored paths
    assert len(Data._data) == 2

    # Mark Data object as temporary
    d_tmp = Data('/path/to/tmp').as_output(tmp=True)
    assert d_tmp.tmp == True
    assert d_tmp.mode == Data.OUTPUT
    assert len(Data._data) == 3

    # Passing '' returns empty string
    d_blank = Data('')
    assert isinstance(d_blank, str)
    assert d_blank == ''
    assert len(Data._data) == 3  # Ensure new Data object was not created


# Parameter objects
def test_parameters():
    p_single = Parameter('one')
    p_multiple = Parameter('-a', 'one', '-b', 'two')
    p_equals = Parameter('--one', 'a', sep='=')
    p_fused = Parameter('-a one -b', 'two')
    p_with_data1 = Parameter(Data('/path/to/data'))
    p_with_data2 = Parameter('-a', Data('/path/to/data1'), Data('/path/to/data2'))

    # Parameters turn into strings as expected
    assert str(p_single) == 'one'
    assert str(p_multiple) == '-a one -b two'
    assert str(p_equals) == '--one=a'  # Use with different separators
    assert str(p_fused) == '-a one -b two'  # Spaces in single string handled correctly

    # Holds Data objects correctly
    assert len(p_single.data) == 0 and not p_single.data
    assert len(p_with_data1.data) == 1
    assert len(p_with_data2.data) == 2
    assert str(p_with_data1.data[0]) == '/path/to/data'
    assert str(p_with_data2) == '-a /path/to/data1 /path/to/data2'


def test_redirects():
    r_norm = Redirect(stream='>', dest='/path/to/dest')
    r_norm_const = Redirect(stream=Redirect.STDOUT, dest='/path/to/dest')
    r_stderr = Redirect(stream='2>', dest='/path/to/dest')
    r_stderr_const = Redirect(stream=Redirect.STDERR, dest='/path/to/dest')
    r_both = Redirect(stream='&>', dest='/path/to/dest')
    r_both_const = Redirect(stream=Redirect.BOTH, dest='/path/to/dest')
    r_null = Redirect(stream='>', dest=Redirect.NULL)
    r_data = Redirect(stream='>', dest=Data('/path/to/data'))

    # Turn into strings as expected
    assert str(r_norm) == '1>/path/to/dest'
    assert str(r_norm_const) == '1>/path/to/dest'
    assert str(r_stderr) == '2>/path/to/dest'
    assert str(r_stderr_const) == '2>/path/to/dest'
    assert str(r_both) == '&>/path/to/dest'
    assert str(r_both_const) == '&>/path/to/dest'

    # Ensure mode is proper
    assert r_norm.mode == 'w'

    # Ensure NULL constant works
    assert str(r_null) == '1>/dev/null'

    # With Data object
    assert isinstance(r_norm.dest, str)
    assert isinstance(r_data.dest, Data)
    assert str(r_data) == '1>/path/to/data'


def test_pipes():
    ParslPipeline._pipeline_run_temp_dir = tempfile.TemporaryDirectory(
        dir='/tmp',
        suffix='__operon'
    )
    one = Software('one', path='/path/to/one')
    pipe = Pipe(one.prep(
        Parameter('-a', '1'),
        Parameter('-b', '2'),
        Redirect(stream='>', dest='/path/to/log')
    ))

    assert isinstance(pipe.piped_software_blueprint, dict)


def test_conda_packages():
    c1 = CondaPackage(tag='star', config_key='STAR')
    assert c1.tag == 'star'
    assert c1.config_key == 'STAR'
    assert c1.executable_path is None
    c2 = CondaPackage(tag='star=2.4.2a', config_key='STAR')
    assert c2.tag == 'star=2.4.2a'
    assert c2.config_key == 'STAR'
    assert c2.executable_path is None
    c3 = CondaPackage(tag='star=2.4.2a', config_key='STAR', executable_path='bin/STAR')
    assert c3.tag == 'star=2.4.2a'
    assert c3.config_key == 'STAR'
    assert c3.executable_path == 'bin/STAR'


def test_software():
    # Reset app_id counter
    _ParslAppBlueprint._id_counter = 0

    # Set temp dir
    ParslPipeline._pipeline_run_temp_dir = tempfile.TemporaryDirectory(
        dir='/tmp',
        suffix='__operon'
    )

    # Spoof pipeline config
    Software._pipeline_config = {
        'software1': {
            'path': '/path/to/soft1'
        },
        'software2': {
            'path': '/path/to/soft2',
            'one': 'two'
        },
        'bwa_mem': {
            'path': '/path/to/bwa mem'
        }
    }

    # Create Software instances
    software1 = Software('software1')
    software2 = Software('software2', subprogram='sub')
    assert software2.basename == 'soft2'
    software3 = Software('software3', path='/path/to/soft3')
    software4 = Software('software4', path='/path/to/soft4', subprogram='four')
    assert software4.path == '/path/to/soft4 four'
    bwa_mem = Software('bwa_mem')
    assert bwa_mem.path == '/path/to/bwa mem'
    assert bwa_mem.basename == 'bwa_mem'

    # Raise ValueError when software path cannot be inferred
    with pytest.raises(ValueError):
        software5 = Software('software5')

    # With different success codes
    software6 = Software('software6', path='/path/to/soft6', success_on=['0', '1', '128'])

    # Registering properly adds to blueprints
    software1.register(
        Parameter('-a', 'one'),
        Redirect(stream='>', dest='/path/to/dest')
    )
    assert len(_ParslAppBlueprint._blueprints) == 1
    assert list(_ParslAppBlueprint._blueprints.keys()) == ['soft1_1']

    # .prep() unique ID per call
    assert software2.prep()['id'] != software2.prep()['id']

    # inputs, outputs, wait_on, stdout, stderr lists properly populated
    # Proper handling of Data objects
    _def_soft3 = software3.register(Parameter('-a', 'one'))
    assert isinstance(_def_soft3, _DeferredApp)
    _soft4_blueprint = software4.prep(
        Parameter('-a', Data('/input1.txt').as_input()),
        Parameter('-b', Data('/input2.txt').as_input()),
        Parameter('--outfile', Data('/output1.txt').as_output()),
        Redirect(stream='>', dest='/stdout.log'),
        Redirect(stream=Redirect.STDERR, dest=Data('/stderr.log')),
        extra_inputs=[Data('/input3.txt'), Data('/input4.txt')],
        extra_outputs=[Data('/output2.txt'), Data('/output3.txt')],
        wait_on=[_def_soft3]
    )
    assert sorted(_soft4_blueprint['inputs']) == ['/input{}.txt'.format(i) for i in range(1, 5)]
    assert sorted(_soft4_blueprint['outputs']) == ['/output{}.txt'.format(i) for i in range(1, 4)] + ['/stderr.log']
    assert len(_soft4_blueprint['wait_on']) == 1
    assert _soft4_blueprint['wait_on'][0] == _def_soft3.app_id
    assert _soft4_blueprint['stdout'] == '/stdout.log'
    assert _soft4_blueprint['stderr'] == '/stderr.log'

    # Proper handling of Pipe
    _piped_soft1_soft6 = software1.prep(
        Parameter('-a', '1'),
        Redirect(stream='>', dest='/ignored.out'),  # This should be overridden by Pipe
        Pipe(software6.prep(
            Parameter('-b', '2'),
            Parameter('-c', Data('/piped.out').as_input()),
            Redirect(stream='>', dest='/piped.log')
        ))
    )
    assert _piped_soft1_soft6['cmd'].count('|') == 1
    assert _piped_soft1_soft6['stdout'] == '/piped.log'
    assert len(_piped_soft1_soft6['inputs']) == 1
    assert _piped_soft1_soft6['inputs'][0] == '/piped.out'

    # Ignore extra Redirects
    _ignore_redir = software2.prep(
        Parameter('-a', '1'),
        Redirect(stream='>', dest='/real.out'),
        Redirect(stream='2>', dest='/real.err'),
        Redirect(stream='>', dest='/ignored.out'),
        Redirect(stream='>', dest='/also_ignored.out'),
        Redirect(stream='2>', dest='/also_ignored.err')
    )
    assert _ignore_redir['stdout'] == '/real.out'
    assert _ignore_redir['stderr'] == '/real.err'

    # Ignore extra pipes
    _ignore_pipe = software2.prep(
        Parameter('-d', 'honeysucklerose'),
        Pipe(software1.prep(
            Parameter('-a', 'real')
        )),
        Pipe(software4.prep(
            Parameter('-b', 'ignored')
        ))
    )
    assert _ignore_pipe['cmd'].count('|') == 1
    assert 'ignored' not in _ignore_pipe['cmd']
    assert 'real' in _ignore_pipe['cmd']

    # Sending stdout/stderr to temporary file
    _stdout_stderr_tmp = software6.prep(
        Parameter('-a', 'ella')
    )
    assert _stdout_stderr_tmp['stdout'] is not None
    assert _stdout_stderr_tmp['stdout'].endswith('.stdout')
    assert _stdout_stderr_tmp['stderr'] is not None
    assert _stdout_stderr_tmp['stderr'].endswith('.stderr')


def test_codeblocks():
    def func1(one, two, three):
        return one ** two + three

    _reg1 = CodeBlock.register(
        func=func1,
        args=(1, 2, 3),
        inputs=[Data('/wait_one.txt')]
    )
    assert isinstance(_reg1, _DeferredApp)

    _reg2 = CodeBlock.register(
        func=func1,
        kwargs={
            'one': 1,
            'two': 2,
            'three': 3
        },
        outputs=[Data('/output_one.txt')],
        stdout='/reg2.out',
        stderr='/reg2.err',
        wait_on=[_reg1]
    )
    _reg1_blueprint = _ParslAppBlueprint._blueprints[_reg1.app_id]
    _reg2_blueprint = _ParslAppBlueprint._blueprints[_reg2.app_id]

    # ID is unique per call
    assert _reg1_blueprint['id'] != _reg2_blueprint['id']

    # Correct reference to function
    assert _reg1_blueprint['func'] is func1
    assert _reg2_blueprint['func'] is func1

    # args and kwargs are both passed correctly
    # inputs, outputs, wait_on, stdout, stderr lists properly populated
    assert _reg1_blueprint['args'] == (1, 2, 3)
    assert _reg1_blueprint['kwargs'] == dict()
    assert _reg2_blueprint['args'] == list()
    assert _reg2_blueprint['kwargs'] == {'one': 1, 'two': 2, 'three': 3}
    assert _reg1_blueprint['inputs'] == ['/wait_one.txt']
    assert _reg2_blueprint['outputs'] == ['/output_one.txt']
    assert len(_reg2_blueprint['wait_on']) == 1
    assert _reg2_blueprint['wait_on'][0] == _reg1_blueprint['id']
    assert _reg2_blueprint['stdout'] == '/reg2.out'
    assert _reg2_blueprint['stderr'] == '/reg2.err'
