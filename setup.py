from setuptools import setup, find_packages

import operon

setup(
    name='Operon',
    version=operon.__version__,
    description='Pipeline design and distribution framework',
    license='MIT',
    author='Dominic Fitzgerald',
    author_email='dominicfitzgerald11@gmail.com',
    url='https://github.com/djf604/operon',
    download_url='https://github.com/djf604/operon/tarball/{}'.format(operon.__version__),
    packages=find_packages(),
    install_requires=['pathos>=0.2.1', 'six', 'libsubmit', 'parsl', 'networkx==2.0'],
    entry_points={
        'console_scripts': [
            'operon = operon.util:execute_from_command_line'
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: MIT License'
    ]
)
