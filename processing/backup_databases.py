import os
import subprocess
from datetime import datetime
import shutil

BACKUP_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'backups')
os.makedirs(BACKUP_DIR, exist_ok=True)

def backup_postgres():
    """
    Backup PostgreSQL database using pg_dump inside the Docker container and copy to BACKUP_DIR
    """
    db_name = os.getenv('POSTGRES_DB', 'openfoodimpact')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    container_name = os.getenv('POSTGRES_CONTAINER', 'projet_certif_cooking-pgvector-1')
    backup_file = f"/tmp/postgres_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    local_backup_file = os.path.join(BACKUP_DIR, os.path.basename(backup_file))
    # Commande de backup dans le conteneur
    cmd = [
        'docker', 'exec', '-e', f'PGPASSWORD={password}', container_name,
        'pg_dump',
        '-U', user,
        '-F', 'c',
        '-b',
        '-f', backup_file,
        db_name
    ]
    print(f"[PostgreSQL] Sauvegarde en cours dans le conteneur {container_name}...")
    subprocess.run(cmd, check=True)
    # Copier le fichier du conteneur vers l'hôte
    print(f"[PostgreSQL] Copie du backup vers {local_backup_file} ...")
    subprocess.run(['docker', 'cp', f'{container_name}:{backup_file}', local_backup_file], check=True)
    # Nettoyer le backup dans le conteneur
    subprocess.run(['docker', 'exec', container_name, 'rm', backup_file], check=True)
    print("[PostgreSQL] Sauvegarde terminée.")

def backup_mongodb():
    """
    Backup MongoDB database using mongodump inside the Docker container and copy to BACKUP_DIR
    """
    db_name = os.getenv('MONGO_INITDB_DATABASE', 'OpenFoodImpact')
    container_name = os.getenv('MONGODB_CONTAINER', 'projet_certif_cooking-mongodb-1')
    backup_dir = f"/tmp/mongodb_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    local_backup_dir = os.path.join(BACKUP_DIR, os.path.basename(backup_dir))
    # Commande de backup dans le conteneur
    cmd = [
        'docker', 'exec', container_name,
        'mongodump',
        '--db', db_name,
        '--out', backup_dir
    ]
    print(f"[MongoDB] Sauvegarde en cours dans le conteneur {container_name}...")
    subprocess.run(cmd, check=True)
    # Copier le dossier du conteneur vers l'hôte
    print(f"[MongoDB] Copie du backup vers {local_backup_dir} ...")
    subprocess.run(['docker', 'cp', f'{container_name}:{backup_dir}', local_backup_dir], check=True)
    # Nettoyer le backup dans le conteneur
    subprocess.run(['docker', 'exec', container_name, 'rm', '-rf', backup_dir], check=True)
    print("[MongoDB] Sauvegarde terminée.")

def restore_postgres(backup_file):
    """
    Restore PostgreSQL database from a backup file using pg_restore inside the Docker container.
    """
    db_name = os.getenv('POSTGRES_DB', 'openfoodimpact')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    container_name = os.getenv('POSTGRES_CONTAINER', 'projet_certif_cooking-pgvector-1')
    container_backup_file = f"/tmp/{os.path.basename(backup_file)}"
    # Copier le fichier de backup dans le conteneur
    print(f"[PostgreSQL] Copie du backup dans le conteneur {container_name} ...")
    subprocess.run(['docker', 'cp', backup_file, f'{container_name}:{container_backup_file}'], check=True)
    # Drop et recreate la base (optionnel, à adapter selon vos besoins)
    print(f"[PostgreSQL] Restauration de la base {db_name} ...")
    drop_cmd = [
        'docker', 'exec', '-e', f'PGPASSWORD={password}', container_name,
        'psql', '-U', user, '-c', f'DROP DATABASE IF EXISTS {db_name}; CREATE DATABASE {db_name};'
    ]
    subprocess.run(drop_cmd, check=True)
    # Restaurer le backup
    restore_cmd = [
        'docker', 'exec', '-e', f'PGPASSWORD={password}', container_name,
        'pg_restore', '-U', user, '-d', db_name, container_backup_file
    ]
    subprocess.run(restore_cmd, check=True)
    # Nettoyer le backup dans le conteneur
    subprocess.run(['docker', 'exec', container_name, 'rm', container_backup_file], check=True)
    print("[PostgreSQL] Restauration terminée.")

def restore_mongodb(backup_dir):
    """
    Restore MongoDB database from a backup directory using mongorestore inside the Docker container.
    """
    db_name = os.getenv('MONGO_INITDB_DATABASE', 'OpenFoodImpact')
    container_name = os.getenv('MONGODB_CONTAINER', 'projet_certif_cooking-mongodb-1')
    container_backup_dir = f"/tmp/{os.path.basename(backup_dir)}"
    # Copier le dossier de backup dans le conteneur
    print(f"[MongoDB] Copie du backup dans le conteneur {container_name} ...")
    subprocess.run(['docker', 'cp', backup_dir, f'{container_name}:{container_backup_dir}'], check=True)
    # Restaurer le backup
    print(f"[MongoDB] Restauration de la base {db_name} ...")
    restore_cmd = [
        'docker', 'exec', container_name,
        'mongorestore', '--db', db_name, '--drop', f'{container_backup_dir}/{db_name}'
    ]
    subprocess.run(restore_cmd, check=True)
    # Nettoyer le backup dans le conteneur
    subprocess.run(['docker', 'exec', container_name, 'rm', '-rf', container_backup_dir], check=True)
    print("[MongoDB] Restauration terminée.")

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
