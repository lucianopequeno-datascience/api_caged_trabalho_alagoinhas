import ftplib
import os
import py7zr
import pandas as pd
from google.cloud import storage
import unicodedata
import shutil

# ==========================================
# Configurações e Variáveis de Ambiente
# ==========================================
BUCKET_NOME = os.getenv("BUCKET_NOME", "seu-bucket-aqui")
ROOT_PATH = os.getenv("ROOT_PATH", "dados")
MUNICIPIO_IBGE = os.getenv("MUNICIPIO_IBGE", "2900702") # Padrão: Alagoinhas
ANO_ALVO = os.getenv("ANO_ALVO", "2026")
TMP_DIR = "/tmp"
FTP_HOST = "ftp.mtps.gov.br"

# ==========================================
# Funções Auxiliares
# ==========================================
def normalize_text(text):
    """Remove acentos e converte para minúsculas para facilitar comparação."""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn').lower()

def get_column_mapping(df_header):
    """Mapeia dinamicamente os nomes das colunas baseados no cabeçalho do arquivo."""
    mapping = {}
    cols_norm = {normalize_text(c): c for c in df_header.columns}
    
    # Colunas exigidas para a análise do CAGED
    required = ['municipio', 'saldomovimentacao', 'competencia', 'cbo2002ocupacao', 'salario', 'idade', 'sexo', 'graudeinstrucao']
    
    for req in required:
        if req in cols_norm:
            mapping[req] = cols_norm[req]
        else:
            for norm, original in cols_norm.items():
                if req in norm:
                    mapping[req] = original
                    break
    return mapping

# ==========================================
# Lógica de Processamento
# ==========================================
def processar_mes_incremental(ano_mes, ftp):
    file_name = f"CAGEDMOV{ano_mes}.7z"
    local_7z_path = os.path.join(TMP_DIR, file_name)
    extract_path = os.path.join(TMP_DIR, f"extracted_{ano_mes}")
    csv_local_path = os.path.join(TMP_DIR, f"caged_{MUNICIPIO_IBGE}_{ano_mes}.csv")
    
    try:
        print(f"\n🚀 Iniciando processamento de: {ano_mes}")
        
        # Tenta navegar para o diretório do mês. Se falhar, o mês ainda não foi publicado.
        try:
            ftp.cwd(f"/pdet/microdados/NOVO CAGED/{ANO_ALVO}/{ano_mes}/")
        except ftplib.error_perm:
            print(f"⚠️ Diretório do mês {ano_mes} não encontrado no FTP. Pulando...")
            return False

        # Download do arquivo compactado
        print(f"📥 Baixando {file_name}...")
        with open(local_7z_path, "wb") as file:
            ftp.retrbinary(f"RETR {file_name}", file.write)
            
        # Extração
        print("📦 Descompactando arquivo...")
        with py7zr.SevenZipFile(local_7z_path, mode='r') as z:
            z.extractall(path=extract_path)
        
        # Busca dinâmica pelo arquivo .txt extraído
        txt_files = [f for f in os.listdir(extract_path) if f.upper().endswith('.TXT')]
        if not txt_files:
            raise FileNotFoundError(f"Nenhum arquivo .txt encontrado dentro de {file_name}")
        txt_file = os.path.join(extract_path, txt_files[0])
        
        # Leitura do cabeçalho
        header_df = pd.read_csv(txt_file, sep=';', nrows=0, encoding='utf-8', encoding_errors='ignore')
        col_map = get_column_mapping(header_df)
        print(f"🔎 Mapeamento de colunas: {col_map}")
        
        if 'municipio' not in col_map:
            raise KeyError("A coluna correspondente a 'municipio' não foi encontrada no cabeçalho.")
        
        # Leitura em chunks com filtro dinâmico
        print(f"⚙️ Processando e filtrando dados para o município {MUNICIPIO_IBGE}...")
        usecols = list(col_map.values())
        chunks_list = []
        
        for chunk in pd.read_csv(txt_file, sep=';', usecols=usecols, chunksize=100000, dtype={col_map['municipio']: str}, encoding='utf-8', encoding_errors='ignore'):
            df_filtered = chunk[chunk[col_map['municipio']] == str(MUNICIPIO_IBGE)]
            if not df_filtered.empty:
                chunks_list.append(df_filtered)
        
        if not chunks_list:
            print(f"⚠️ Nenhum registro encontrado para o município {MUNICIPIO_IBGE} no mês {ano_mes}.")
            return True # Retorna True pois rodou com sucesso, apenas não havia dados locais
            
        # Consolidação e salvamento
        df_final = pd.concat(chunks_list, ignore_index=True)
        df_final.to_csv(csv_local_path, index=False)
        
        # Upload para o Google Cloud Storage
        print("☁️ Subindo dados para o Cloud Storage...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NOME)
        blob = bucket.blob(f"{ROOT_PATH}/landing/caged/ano_mes={ano_mes}/caged_{MUNICIPIO_IBGE}_{ano_mes}.csv")
        blob.upload_from_filename(csv_local_path)
        
        print(f"✅ Sucesso: {ano_mes} concluído!")
        return True
        
    except Exception as e:
        print(f"❌ Erro crítico no processamento de {ano_mes}: {e}")
        raise # Força o container a registrar falha real (exit 1)
        
    finally:
        # Limpeza rigorosa do diretório temporário para evitar estouro de memória no Cloud Run
        for p in [local_7z_path, csv_local_path]:
            if os.path.exists(p): 
                os.remove(p)
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)

# ==========================================
# Orquestrador Principal
# ==========================================
def main():
    print("==================================================")
    print("   INICIANDO PIPELINE DE INGESTÃO DO CAGED        ")
    print("==================================================")
    
    ftp = None
    try:
        # Conexão centralizada no FTP
        print(f"Conectando ao FTP do governo: {FTP_HOST}...")
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login() 
        print("Conexão FTP estabelecida.")
        
        # Gera a lista de meses do ano atual (01 a 12)
        meses = [f"{i:02d}" for i in range(1, 13)]
        
        for mes in meses:
            ano_mes = f"{ANO_ALVO}{mes}"
            sucesso = processar_mes_incremental(ano_mes, ftp)
            
            # Opcional: Se a função retornar False (diretório não existe), 
            # você pode decidir interromper o loop, pois os meses seguintes também não existirão.
            if sucesso is False and int(mes) > 1:
                print("Fim dos dados disponíveis no servidor para este ano.")
                break
                
    except Exception as e:
        print(f"💥 Falha na execução principal: {e}")
        raise
    finally:
        if ftp:
            try:
                ftp.quit()
                print("Conexão FTP encerrada.")
            except:
                pass
    
    print("Pipeline finalizado.")

# ==========================================
# Ponto de Entrada do Script
# ==========================================
if __name__ == "__main__":
    main()
