import os
import glob
import subprocess


def operon_run(tmp_dir):
    # Uninstall dependencies for test, if they're installed
    subprocess.call('operon run testseq_pipeline --test {}'.format(tmp_dir), shell=True)
    assert glob.glob('*.operon.log')
    assert subprocess.call('grep @operon {}'.format(glob.glob('*.operon.log')[0]), shell=True) == 0
    subprocess.call('rm -rf *.operon.log', shell=True)
    return True


def operon_configure(operon_root):
    p = subprocess.Popen('operon configure testseq_pipeline', shell=True, stdin=subprocess.PIPE)
    # If the configuration dictionary changes so will the order of input here
    p.communicate(input='Y\r\r/usr/bin/wget\r/bin/sleep\r'
                        '/home/dfitzgerald/workspace/PycharmProjects/Operon/tests/ersatz.py\r'.encode())
    # TODO Figure out a way to get this to work
    assert os.path.isfile(os.path.join(operon_root, '.operon', 'configs', 'testseq_pipeline.json'))
    return True


def operon_install(operon_root):
    # pip(['uninstall', '-y', 'pyvcf'])
    tests_dir = os.path.dirname(__file__)
    subprocess.call('operon install -y {}'.format(
        os.path.join(tests_dir, 'testseq_pipeline.py')
    ), shell=True)
    assert os.path.isfile(os.path.join(operon_root, '.operon', 'pipelines', 'testseq_pipeline.py'))
    return True


def operon_init(args='', tmpdir=None, cleanup=True):
    subprocess.call('operon init {}'.format(args), shell=True)

    if args != 'help':
        operon_home = os.path.join(tmpdir or args, '.operon')
        assert os.path.isfile(os.path.join(operon_home, 'pipelines', '__init__.py'))
        assert os.path.isfile(os.path.join(operon_home, 'configs', '__init__.py'))
        assert os.path.isfile(os.path.join(operon_home, '.operon_completer'))
        assert os.stat(os.path.join(operon_home, '.operon_completer')).st_mode & 0o777 == 0o755
        assert os.path.isfile(os.path.join(operon_home, 'operon_state.json'))
        assert os.path.isfile(os.path.join(operon_home, 'parsl_config.json'))

        # Assert ~/.bash_completion was written out
        with open(os.path.join(os.path.expanduser('~'), '.bash_completion')) as bash_completion:
            assert '# Added by Operon pipeline development package' in bash_completion.read()

        # Assert OPERON_HOME export was written out
        if (tmpdir or args) != os.path.expanduser('~') and not os.environ.get('OPERON_HOME'):
            for preload_file in ('.bashrc', '.bash_profile'):
                if os.path.isfile(os.path.join(os.path.expanduser('~'), preload_file)):
                    break
            else:
                # If none of the above files were found, default to .profile
                preload_file = '.profile'

            with open(os.path.join(os.path.expanduser('~'), preload_file)) as export_file:
                assert 'export OPERON_HOME=' in export_file.read()

        if cleanup:
            subprocess.call('rm -rf {}'.format(tmpdir or args), shell=True)
    return True



def test_cli(tmpdir_factory):
    assert operon_init(args='help')
    assert operon_init(args=str(tmpdir_factory.mktemp('user_defined')))

    spoofed_home = str(tmpdir_factory.mktemp('home'))
    os.environ['HOME'] = spoofed_home
    assert operon_init(tmpdir=spoofed_home, cleanup=False)

    operon_root = str(tmpdir_factory.mktemp('operon_root'))
    os.environ['OPERON_HOME'] = operon_root
    assert operon_init(tmpdir=operon_root, cleanup=False)

    assert operon_install(operon_root)
    assert operon_configure(operon_root)

    assert operon_run(str(tmpdir_factory.mktemp('run')))
