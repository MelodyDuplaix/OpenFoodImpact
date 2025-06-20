import os
import subprocess
from datetime import datetime

BACKUP_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'backups')
os.makedirs(BACKUP_DIR, exist_ok=True)

def backup_postgres():
    """
    Backup PostgreSQL database using pg_dump to a compressed file in the BACKUP_DIR
    """
    db_name = os.getenv('POSTGRES_DB', 'openfoodimpact')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    backup_file = os.path.join(BACKUP_DIR, f"postgres_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql")
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    cmd = [
        'pg_dump',
        '-h', host,
        '-p', port,
        '-U', user,
        '-F', 'c',  # format compact
        '-b',       # on inclut les blobs
        '-f', backup_file,
        db_name
    ]
    print(f"[PostgreSQL] Sauvegarde en cours vers {backup_file} ...")
    subprocess.run(cmd, env=env, check=True)
    print("[PostgreSQL] Sauvegarde terminée.")

def backup_mongodb():
    """
    Backup MongoDB database using mongodump to a directory in the BACKUP_DIR
    """
    db_name = os.getenv('MONGO_INITDB_DATABASE', 'OpenFoodImpact')
    host = os.getenv('MONGODB_HOST', 'localhost')
    port = os.getenv('MONGODB_PORT', '27017')
    backup_path = os.path.join(BACKUP_DIR, f"mongodb_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    cmd = [
        'mongodump',
        '--host', host,
        '--port', port,
        '--db', db_name,
        '--out', backup_path
    ]
    print(f"[MongoDB] Sauvegarde en cours vers {backup_path} ...")
    subprocess.run(cmd, check=True)
    print("[MongoDB] Sauvegarde terminée.")

def main():
    try:
        backup_postgres()
    except Exception as e:
        print(f"Erreur lors de la sauvegarde PostgreSQL : {e}")
    try:
        backup_mongodb()
    except Exception as e:
        print(f"Erreur lors de la sauvegarde MongoDB : {e}")

if __name__ == "__main__":
    main()
