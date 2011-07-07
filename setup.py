from setuptools import setup, find_packages

setup(
    name = 'webstore',
    version = '0.2',
    packages = find_packages(),
    install_requires = [
        ],
    # metadata for upload to PyPI
    author = 'Open Knowledge Foundation',
    author_email = 'info@okfn.org',
    description = '',
    license = 'MIT',
    url = '',
    download_url = '',
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
    '''
)


