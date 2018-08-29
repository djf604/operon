# First option is always the default
schema = {
    'no_parsl_config_behavior': [
        {
            'option': 'use_package_default',
            'readable': 'Use package default',
            'description': 'Use the Operon package default: 2 threads'
        },
        {
            'option': 'fail',
            'readable': 'Fail',
            'description': 'Fail the run immediately'
        }
    ],
    'delete_temporary_files': [
        {
            'option': 'yes',
            'readable': 'Yes',
            'description': 'Delete files marked temporary at the end of a run'
        },
        {
            'option': 'no',
            'readable': 'No',
            'description': 'Keep all files generated during a run, even those marked temporary'
        }
    ]
}
