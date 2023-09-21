from setuptools import setup, find_packages
from pathlib import Path

VERSION = '0.1.4' 
DESCRIPTION = 'MOEX Importer package'

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
	name='moeximporter', 
	version=VERSION,
	author='Konstantin Novik',
	author_email='<konstantin.novik@gmail.com>',
	license='MIT',
    url='https://github.com/cdracer/moex-importer',
	description=DESCRIPTION,
	long_description=long_description,
 	long_description_content_type='text/markdown',
	packages=find_packages(),
	install_requires=['pandas', ],
	readme='README.md',
	keywords=['python', 'MOEX', 'MOEX quotes', 'finance'],
	classifiers= [
		'License :: OSI Approved :: MIT License',
		'Development Status :: 4 - Beta',
		'Intended Audience :: Financial and Insurance Industry',
		'Programming Language :: Python :: 3',
		'Operating System :: MacOS :: MacOS X',
		'Operating System :: Microsoft :: Windows',
		'Operating System :: POSIX',
        ]
)
