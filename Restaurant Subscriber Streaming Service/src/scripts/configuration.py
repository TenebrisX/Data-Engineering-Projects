import os

#kafka config
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') 
KAFKA_SECURITY_PROTOCOL = os.environ.get('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-512')
KAFKA_SASL_CONFIG = os.environ.get('KAFKA_SASL_CONFIG',  'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password=\"ltcneltyn\";')

#postgres config
POSTGRES_URL =  os.environ.get('POSTGRES_URL', "jdbc:postgresql://localhost:5432/de")
POSTGRES_DRIVER = 'org.postgresql.Driver'
POSTGRES_USER_CLOUD =  os.environ.get('POSTGRES_USER_CLOUD', "student")
POSTGRES_PASSWORD_CLOUD = os.environ.get('POSTGRES_PASSWORD_CLOUD',  "de-student")
POSTGRES_USER_LOCAL =  os.environ.get('POSTGRES_USER_LOCAL', "jovyan")
POSTGRES_PASSWORD_LOCAL = os.environ.get('POSTGRES_PASSWORD_LOCAL',  "jovyan")

#topic names
TOPIC_NAME_IN = 'kafka.kotlyarovb_in'
TOPIC_NAME_OUT = 'kafka.kotlyarovb_out'