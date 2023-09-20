from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'MOEX Importer package'
LONG_DESCRIPTION = 'Import quotes and data from MOEX ISS API'

setup(
	name='moeximporter', 
	version=VERSION,
	author='Konstantin Novik',
	author_email='<konstantin.novik@gmail.com>',
	license='MIT',
	description=DESCRIPTION,
	long_description=LONG_DESCRIPTION,
	packages=find_packages(),
	install_requires=['pandas', ],
	readme='README.md',
	keywords=['python', 'MOEX', 'MOEX quotes', 'finance'],
	classifiers= [
		'License :: OSI Approved :: MIT License',
		'Development Status :: 3 - Alpha',
		'Intended Audience :: Finance',
		'Programming Language :: Python :: 3',
		'Operating System :: MacOS :: MacOS X',
		'Operating System :: Microsoft :: Windows',
		'Operating System :: POSIX',
        ]
)
