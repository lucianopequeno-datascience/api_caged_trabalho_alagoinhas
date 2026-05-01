import ftplib
import os
import py7zr
import pandas as pd
from datetime import datetime
from google.cloud import storage

# Variáveis de Ambiente
BUCKET_NOME = os.getenv("BUCKET_NOME")
ROOT_PATH = os.getenv("ROOT_PATH", "") # pasta dentro do bucket
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702")
ANO_ALVO = os.getenv("ANO_ALVO", "2026")
TMP_DIR = "/tmp"

def obter_meses_processados():
    print("🔍 Verificando Data Lake no path:", f"{ROOT_PATH}/landing/caged/")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NOME)
    
    # Busca apenas dentro do prefixo definido
    prefixo_busca = f"{ROOT_PATH}/landing/caged/"
    blobs = bucket.list_blobs(prefix=prefixo_busca)
    
    meses_existentes = set()
    for blob in blobs:
        if "ano_mes=" in blob.name:
            pasta_ano_mes = blob.name.split("ano_mes=")[1].split("/")[0]
            if pasta_ano_mes.startswith(ANO_ALVO):
                meses_existentes.add(pasta_ano_mes)
    return meses_existentes

def gerar_meses_esperados():
    mes_atual = datetime.now().month
    meses_esperados = set()
    for m in range(1, mes_atual + 1):
        meses_esperados.add(f"{ANO_ALVO}{m:02d}")
    return meses_esperados

def processar_mes_incremental(ano_mes, ftp):
    ftp_dir = f"/pdet/microdados/NOVO CAGED/{ANO_ALVO}/{ano_mes}/"
    file_name = f"CAGEDEST{ano_mes}.7z"
    local_7z_path = os.path.join(TMP_DIR, file_name)
    extract_path = os.path.join(TMP_DIR, f"extracted_{ano_mes}")
    
    print(f"\n🚀 Processando mês: {ano_mes}")
    
    try:
        ftp.cwd(ftp_dir)
        with open(local_7z_path, "wb") as file:
            ftp.retrbinary(f"RETR {file_name}", file.write)
    except Exception as e:
        print(f"⚠️ Mês {ano_mes} ainda não disponível no MTE. Pulando...")
        return
        
    with py7zr.SevenZipFile(local_7z_path, mode='r') as z:
        z.extractall(path=extract_path)
        
    txt_file = os.path.join(extract_path, file_name.replace('.7z', '.txt'))
    
    colunas = ['competência', 'município', 'saldomovimentação', 'cbo2002ocupação', 'salário', 'idade', 'sexo', 'graudeinstrução']
    df_cidade = pd.DataFrame()
    
    # Processa em chunks para não estourar memória
    for chunk in pd.read_csv(txt_file, sep=';', usecols=colunas, chunksize=100000, dtype={'município': str}):
        df_cidade = pd.concat([df_cidade, chunk[chunk['município'] == MUNICIPIO_IBGE]])
        
    csv_filename = f"caged_{MUNICIPIO_IBGE}_{ano_mes}.csv"
    csv_local_path = os.path.join(TMP_DIR, csv_filename)
    df_cidade.to_csv(csv_local_path, index=False)
    
    # Upload corrigido com o ROOT_PATH
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NOME)
    blob_path = f"{ROOT_PATH}/landing/caged/ano_mes={ano_mes}/{csv_filename}"
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(csv_local_path)
    
    os.remove(local_7z_path)
    os.remove(csv_local_path)
    print(f"✅ Salvo em: {blob_path}")

def main():
    meses_processados = obter_meses_processados()
    meses_esperados = gerar_meses_esperados()
    meses_pendentes = sorted(list(meses_esperados - meses_processados))
    
    if not meses_pendentes:
        print("✅ Data Lake atualizado.")
        return
        
    ftp = ftplib.FTP("ftp.mtps.gov.br")
    ftp.login()
    for ano_mes in meses_pendentes:
        processar_mes_incremental(ano_mes, ftp)
    ftp.quit()

if __name__ == "__main__":
    main()