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


def create_sink_postgres(t_env):
    table_name = "tumbling_pu_location"
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        window_start   TIMESTAMP(3),
        PULocationID   INT,
        num_trips      BIGINT,
        PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
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
    # === Setup environments ===
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)          # 10 seconds checkpointing
    env.set_parallelism(1)                   # IMPORTANT: only 1 partition in topic

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # === Create source & sink ===
    source_table = create_events_source_kafka(t_env)
    sink_table = create_sink_postgres(t_env)

    # === The aggregation query ===
    # 5-minute tumbling window on event time
    insert_stmt = f"""
    INSERT INTO {sink_table}
    SELECT
        window_start,
        PULocationID,
        COUNT(*) AS num_trips
    FROM TABLE(
        TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, PULocationID;
    """

    # Execute and wait (streaming job runs continuously)
    t_env.execute_sql(insert_stmt).wait()


if __name__ == '__main__':
    main()