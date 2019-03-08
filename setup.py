from setuptools import setup

setup(
    name='JsvKafkaConsumer',
    url='https://github.com/Statoil/JsvKafkaConsumer',
    version='0.1',
    author='Lindvar Lagran',
    author_email='llag@equinor.com',
    install_requires=['confluent-kafka','fastavro', 'avro-python3', 'requests'],
    packages=['jskafka', 'jskafka.module'],
    test_suite='nose.collector',
    tests_require=['nose'],
)