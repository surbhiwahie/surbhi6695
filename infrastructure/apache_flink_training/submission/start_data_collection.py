from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment

def create_processed_events_sink_kafka(t_env):
    table_name = "process_events_kafka"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SSL',
            'properties.ssl.truststore.location' = '/var/private/ssl/kafka_truststore.jks',
            'properties.ssl.truststore.password' = '{os.environ.get("KAFKA_PASSWORD")}',
            'properties.ssl.keystore.location' = '/var/private/ssl/kafka_client.jks',
            'properties.ssl.keystore.password' = '{os.environ.get("KAFKA_PASSWORD")}',
            'format' = 'json'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SSL',
            'properties.ssl.truststore.location' = '/var/private/ssl/kafka_truststore.jks',
            'properties.ssl.truststore.password' = '{os.environ.get("KAFKA_PASSWORD")}',
            'properties.ssl.keystore.location' = '/var/private/ssl/kafka_client.jks',
            'properties.ssl.keystore.password' = '{os.environ.get("KAFKA_PASSWORD")}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        kafka_sink = create_processed_events_sink_kafka(t_env)

        # write records to Kafka first
        t_env.execute_sql(
            f"""
            INSERT INTO {kafka_sink}
            SELECT
                JSON_VALUE(headers, '$.x-forwarded-for') as ip,
                DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss') AS event_timestamp,
                referrer,
                host,
                url,
                get_location(JSON_VALUE(headers, '$.x-forwarded-for')) as geodata
            FROM {source_table}
            """
        )

        # write records to postgres too!
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        JSON_VALUE(headers, '$.x-forwarded-for') as ip,
                        event_timestamp,
                        referrer,
                        host,
                        url
                    FROM {source_table}
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
