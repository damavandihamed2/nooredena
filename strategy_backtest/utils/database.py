import socket, pyodbc


ip = "172.20.30.34"
authenticators = {"public": {"username": "userpublic", "password": "Nooredena@public.123"}}
authenticator_public = authenticators["public"]


def make_connection(username: str, password: str) -> pyodbc.Connection:
    conn_str = (f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER=172.20.30.34; DATABASE=nooredenadb; "
                f"UID={username}; PWD={password}; Encrypt=No")
    db_conn = pyodbc.connect(conn_str)
    return db_conn


def db_conn_check():
    try:
        conn = socket.create_connection((ip, 1433), timeout=3)
        conn.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


if db_conn_check():
    powerbi_database_public = make_connection(
        username=authenticator_public["username"], password=authenticator_public["password"]
    )
else:
    powerbi_database_public = None

