import psycopg2
import json

from aviso.framework.connection_factory import ConnectionFactory

def get_oppds_config(tenant_name, service):
    # note: since we are using pgbouncer at the front this should be fine.
    # TODO: find a way to use a connection pool and db access should be handled via a context manager.
    supported_services = ['gbm', 'etl']
    if service not in supported_services:
        raise ValueError(f"service must be one of {supported_services}")
    stack = ConnectionFactory._get_ms_stack(tenant_name, service)
    conn = ConnectionFactory._get_pg_conn(db_type=service, stack=stack)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT value 
                FROM tenant_configs
                JOIN tenant on tenant_configs.tenant_id = tenant._id
                WHERE tenant.name = %s AND category = 'datasets' AND config_name = 'OppDS'
                """,
                (tenant_name,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No OppDS config found for tenant: {tenant_name}")
    finally:
        conn.close()
    try:
        oppds_raw_config = json.loads(row["value"]) # this is stored as a string in the db
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON found in OppDS config for tenant: {tenant_name}")
    except Exception as e:
        raise ValueError(f"Unexpected error while parsing OppDS config for tenant: {tenant_name}. Error: {str(e)}")
    return oppds_raw_config