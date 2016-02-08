try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

INSTALL_REQUIRE = []
with open('requirements/prod.txt') as fd:
     for line in fd.readlines():
        INSTALL_REQUIRE.append(line.replace(' ', ''))


TEST_REQUIRE = []
with open('requirements/dev.txt') as fd:
     for line in fd.readlines():
        TEST_REQUIRE.append(line.replace(' ', ''))

setup(
    name='outbrain',
    version='0.0.10',
    author='Brian Mason',
    author_email='bmason@simplereach.com',
    url='https://github.com/bmason/python-outbrain',
    packages=['outbrain'],
    license='LGPL 2.1 or later',
    description='Wrapper for the Outbrain Amplify API',
    install_requires=INSTALL_REQUIRE,
    tests_require=TEST_REQUIRE,
    test_suite='test',
    keywords=['outbrain','api', 'amplify'],
    classifiers=['Intended Audience :: Developers']
)
