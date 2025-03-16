import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
print("Carregando variáveis de ambiente do arquivo .env...")
load_dotenv()

# Verificar o valor da variável DB_NAME
db_name = os.getenv('DB_NAME')
print(f"Valor de DB_NAME: {db_name}")

# Verificar o conteúdo do arquivo .env
print("\nConteúdo do arquivo .env:")
try:
    with open('.env', 'r') as f:
        print(f.read())
except Exception as e:
    print(f"Erro ao ler o arquivo .env: {e}")
