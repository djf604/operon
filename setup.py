from setuptools import setup, find_packages

import operon

setup(
    name='Operon',
    version=operon.__version__,
    description='Dataflow pipeline development framework, powered by Parsl',
    license='GNU GPLv3',
    author='Dominic Fitzgerald',
    author_email='dominicfitzgerald11@gmail.com',
    url='https://github.com/djf604/operon',
    download_url='https://github.com/djf604/operon/tarball/{}'.format(operon.__version__),
    packages=find_packages(),
    install_requires=['libsubmit', 'parsl==0.3.1', 'networkx==2.0'],
    entry_points={
        'console_scripts': [
            'operon = operon._util:execute_from_command_line'
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'
    ]
)
