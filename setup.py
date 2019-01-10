from setuptools import setup

setup(
    name='JsvKafkaConsumer',
    url='https://github.com/Statoil/JsvKafkaConsumer',
    version='0.1',
    author='Lindvar Lægran',
    author_email='llag@equinor.com',
    install_requires=['kafka-python','fastavro'],
    packages=['jskafka'],
    test_suite='nose.collector',
    tests_require=['nose'],
)