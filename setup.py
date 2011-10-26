from setuptools import setup, find_packages

setup(
    name = 'webstore',
    version = '0.3a',
    packages = find_packages(),
    install_requires = [
        'python-dateutil>=1.2',
        'SQLAlchemy==0.7.3',
        'Flask==0.7.1',
        'sqlalchemy-migrate>=0.7.1'
        ],
    # metadata for upload to PyPI
    author = 'Open Knowledge Foundation',
    author_email = 'info@okfn.org',
    description = 'webstore is a RESTful data store for tabular and table-like data.',
    long_description = '''Webstore can be used
as a dynamic storage for table data, allowing filtered, partial or full
retrieval and format conversion.

See the full documentation at: http://webstore.readthedocs.org/en/latest/
    ''',
    license = 'MIT',
    url = 'http://webstore.thedatahub.org/',
    download_url = '',
    include_package_data = True,
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    entry_points = '''
    [webstore.auth]
    always_login = webstore.security:always_login
    never_login = webstore.security:never_login
    ckan = webstore.ckan:check_ckan_login
    sw_auth = webstore.swauth:sw_auth

    [webstore.authz]
    default = webstore.security:default_has
    sw_has = webstore.swauth:sw_has
    '''
)


