from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_source_kafka(t_env):
    table_name = "green_trips_source"
    source_ddl = f"""
    CREATE TABLE {table_name} (
        PULocationID      INT,
        DOLocationID      INT,
        trip_distance     DOUBLE,
        total_amount      DOUBLE,
        lpep_pickup_datetime VARCHAR,
        event_timestamp   AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_session_sink_postgres(t_env):
    table_name = "session_pu_location"
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        PULocationID     INT,
        window_start     TIMESTAMP(3),
        window_end       TIMESTAMP(3),
        num_trips        BIGINT,
        PRIMARY KEY (PULocationID, window_start, window_end) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = '{table_name}',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    sink_table = create_session_sink_postgres(t_env)

    insert_stmt = f"""
    INSERT INTO {sink_table}
    SELECT
        PULocationID,
        SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
        SESSION_END(event_timestamp, INTERVAL '5' MINUTE)   AS window_end,
        COUNT(*) AS num_trips
    FROM {source_table}
    GROUP BY
        PULocationID,
        SESSION(event_timestamp, INTERVAL '5' MINUTE);
    """

    t_env.execute_sql(insert_stmt).wait()

if __name__ == '__main__':
    main()