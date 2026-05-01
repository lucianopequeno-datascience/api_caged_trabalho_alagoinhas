import ftplib
import os
import py7zr
import pandas as pd
from datetime import datetime
from google.cloud import storage
import unicodedata

# Variáveis de Ambiente
BUCKET_NOME = os.getenv("BUCKET_NOME")
ROOT_PATH = os.getenv("ROOT_PATH", "")
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702")
ANO_ALVO = os.getenv("ANO_ALVO", "2026")
TMP_DIR = "/tmp"

def normalize_text(text):
    """Remove acentos e minúsculas para facilitar comparação."""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn').lower()

def get_column_mapping(df_header):
    """Mapeia dinamicamente os nomes das colunas baseados no arquivo."""
    # Mapeamento: {'nome_esperado_normalizado': 'nome_real_no_arquivo'}
    mapping = {}
    cols_norm = {normalize_text(c): c for c in df_header.columns}
    
    # Nomes que buscamos
    required = ['municipio', 'saldomovimentacao', 'competencia', 'cbo2002ocupacao', 'salario', 'idade', 'sexo', 'graudeinstrucao']
    
    for req in required:
        if req in cols_norm:
            mapping[req] = cols_norm[req]
        else:
            # Tenta encontrar correspondente (ex: 'municipio' -> 'município')
            for norm, original in cols_norm.items():
                if req in norm:
                    mapping[req] = original
                    break
    return mapping

def processar_mes_incremental(ano_mes, ftp):
    file_name = f"CAGEDMOV{ano_mes}.7z"
    local_7z_path = os.path.join(TMP_DIR, file_name)
    extract_path = os.path.join(TMP_DIR, f"extracted_{ano_mes}")
    csv_local_path = os.path.join(TMP_DIR, f"caged_{MUNICIPIO_IBGE}_{ano_mes}.csv")
    
    try:
        print(f"\n🚀 Iniciando: {ano_mes}")
        ftp.cwd(f"/pdet/microdados/NOVO CAGED/{ANO_ALVO}/{ano_mes}/")
        with open(local_7z_path, "wb") as file:
            ftp.retrbinary(f"RETR {file_name}", file.write)
            
        with py7zr.SevenZipFile(local_7z_path, mode='r') as z:
            z.extractall(path=extract_path)
        
        txt_file = os.path.join(extract_path, file_name.replace('.7z', '.txt'))
        
        # 1. Leitura do cabeçalho para descobrir nomes das colunas
        header_df = pd.read_csv(txt_file, sep=';', nrows=0)
        col_map = get_column_mapping(header_df)
        print(f"DEBUG: Mapeamento de colunas: {col_map}")
        
        # 2. Leitura com chunksize e filtro dinâmico
        df_final = pd.DataFrame()
        usecols = list(col_map.values())
        
        for chunk in pd.read_csv(txt_file, sep=';', usecols=usecols, chunksize=100000, dtype={col_map['municipio']: str}):
            df_filtered = chunk[chunk[col_map['municipio']] == MUNICIPIO_IBGE]
            df_final = pd.concat([df_final, df_filtered])
            
        df_final.to_csv(csv_local_path, index=False)
        
        # Upload
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NOME)
        blob = bucket.blob(f"{ROOT_PATH}/landing/caged/ano_mes={ano_mes}/caged_{MUNICIPIO_IBGE}_{ano_mes}.csv")
        blob.upload_from_filename(csv_local_path)
        print(f"✅ Sucesso: {ano_mes}")
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
    finally:
        # Garante a limpeza mesmo se der erro
        for p in [local_7z_path, csv_local_path]:
            if os.path.exists(p): os.remove(p)

def main():
    # ... (manter o restante igual)
    # Certifique-se apenas de chamar o processar_mes_incremental com a lógica ftp correta
    pass
