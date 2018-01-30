var typed = new Typed('#typed', {
    strings: [
        '$ operon install /path/to/pipeline.py^500\n' +
        '`> Pipeline pipeline.py successfully installed.`^2000\n' +
        '`$ `operon configure pipeline^500\n' +
        '`Conda is installed, but no environment has been created for this\n' +
        'pipeline. Operon can use conda to download the software this\n' +
        'pipeline uses and inject those into your configuration. Would you\n' +
        'like to download the software now? [y/n]`^1000 y^500\n' +
        '`|software1|Full path to software1 []: `^800 /path/to/software1^500\n' +
        '`|software1|Threads to run software1 []: `^800 4^500\n' +
        '`|software2|Full path to software2 []: `^800 /path/to/software2^500\n' +
        '`Configuration file successfully written.`^2000\n' +
        '`$ `operon run pipeline -a 10 -b 20 --another-option bubbles^500\n' +
        '`Running pipeline...`^1000\n`...`^1000\n`...`^1000\n`...`^1000\n`Done!`'
    ],
    typeSpeed: 0,
    showCursor: false,
    loop: true,
    loopCount: 3,
    fadeOut: true
});
// typed.stop();