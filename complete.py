import pandas as pd
import pyodbc
from google.oauth2 import service_account
from googleapiclient.discovery import build
import io
from googleapiclient.http import MediaIoBaseDownload

SERVICE_ACCOUNT_FILE = 'C:\\Users\\thorodin\\Desktop\\api_google\\<arquivo>.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

def export_and_download_file(file_id, mime_type, destination_file_name):
    try:
        request = service.files().export_media(fileId=file_id, mimeType=mime_type)
        fh = io.FileIO(destination_file_name, mode='wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f'Download Progress: {int(status.progress() * 100)}%')
        print(f'Download Complete: {destination_file_name}')
    except Exception as e:
        print(f'An error occurred: {e}')

def migrate_sheet_to_xls(xlsx_filename, sheet_name):
    try:
        df = pd.read_excel(xlsx_filename, sheet_name=sheet_name, engine='openpyxl')
        xls_filename = xlsx_filename.replace('.xlsx', '.xls')
        with pd.ExcelWriter(xls_filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        print(f'Sheet "{sheet_name}" do arquivo {xlsx_filename} migrada para {xls_filename}')
    except FileNotFoundError:
        print(f'O arquivo {xlsx_filename} não foi encontrado.')
    except Exception as e:
        print(f'Ocorreu um erro durante a migração da sheet "{sheet_name}": {str(e)}')

def load_excel_data(file_path):
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip()  # Remove espaços em branco dos nomes das colunas
    return df

def connect_to_db():
    server = 'SERVDB4\\SQLEXPRESS'
    database = 'censured'
    username = 'censured'
    password = 'censured'
    driver = 'ODBC Driver 17 for SQL Server'
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)
    return conn

def upsert_data(conn, df):
    cursor = conn.cursor()
    date_columns = ['Data da Reclamação', 'Data da moderação']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')

    for index, row in df.iterrows():
        try:
            id = str(row['ID']) if not pd.isna(row['ID']) else None
            ano = str(row['Ano']) if not pd.isna(row['Ano']) else None
            mes = str(row['Mês']) if not pd.isna(row['Mês']) else None
            data_reclamacao = row['Data da Reclamação'] if not pd.isna(row['Data da Reclamação']) else None
            solucao_mes = str(row['Solução mês']) if not pd.isna(row['Solução mês']) else None
            nome_consumidor = str(row['Nome do consumidor']) if not pd.isna(row['Nome do consumidor']) else None
            descr_legenda = str(row['Motivo / Descrição / Legenda da reclamação']) if not pd.isna(row['Motivo / Descrição / Legenda da reclamação']) else None
            ocorrencia = str(row['Ocorrência']) if not pd.isna(row['Ocorrência']) else None
            produto = str(row['Produto reclamado']) if not pd.isna(row['Produto reclamado']) else None
            nivel = str(row['Nível']) if not pd.isna(row['Nível']) else None
            replic = str(row['Réplica']) if not pd.isna(row['Réplica']) else None
            resposta_publica = str(row['Resposta Pública']) if not pd.isna(row['Resposta Pública']) else None
            avaliacao = str(row['Avaliação']) if not pd.isna(row['Avaliação']) else None
            nota = str(row['Nota']) if not pd.isna(row['Nota']) else None
            voltaria_fazer_negocio = str(row['Voltaria a fazer negócio?']) if not pd.isna(row['Voltaria a fazer negócio?']) else None
            caso_resolvido = str(row['Caso Resolvido?']) if not pd.isna(row['Caso Resolvido?']) else None
            o_que_houve_no_atendimento = str(row['O que houve no atendimento?']) if not pd.isna(row['O que houve no atendimento?']) else None
            moderacao_solicitada = str(row['Moderação Solicitada']) if not pd.isna(row['Moderação Solicitada']) else None
            data_moderacao = row['Data da moderação'] if not pd.isna(row['Data da moderação']) else None
            motivo = str(row['Motivo']) if not pd.isna(row['Motivo']) else None
            moderacao_aceita = str(row['Moderação aceita']) if not pd.isna(row['Moderação aceita']) else None
            motivo_negativa = str(row['Motivo da negativa']) if not pd.isna(row['Motivo da negativa']) else None

            cursor.execute("SELECT COUNT(*) FROM reclameAqui WHERE id = ?", id)
            if cursor.fetchone()[0] > 0:
                cursor.execute("""UPDATE reclameAqui SET ano = ?, mes = ?, data_reclamacao = ?, solucao_mes = ?, nome_consumidor = ?, 
                                  descr_legenda = ?, ocorrencia = ?, produto = ?, nivel = ?, replic = ?, resposta_publica = ?, 
                                  avaliacao = ?, nota = ?, voltaria_fazer_negocio = ?, caso_resolvido = ?, o_que_houve_no_atendimento = ?, 
                                  moderacao_solicitada = ?, data_moderacao = ?, motivo = ?, moderacao_aceita = ?, motivo_negativa = ? 
                                  WHERE id = ?""",
                               ano, mes, data_reclamacao, solucao_mes, nome_consumidor, descr_legenda, ocorrencia, produto, nivel, replic, resposta_publica, 
                               avaliacao, nota, voltaria_fazer_negocio, caso_resolvido, o_que_houve_no_atendimento, moderacao_solicitada, data_moderacao, motivo, 
                               moderacao_aceita, motivo_negativa, id)
            else:
                cursor.execute("""INSERT INTO reclameAqui (id, ano, mes, data_reclamacao, solucao_mes, nome_consumidor, descr_legenda, ocorrencia, produto, nivel, 
                                  replic, resposta_publica, avaliacao, nota, voltaria_fazer_negocio, caso_resolvido, o_que_houve_no_atendimento, moderacao_solicitada, 
                                  data_moderacao, motivo, moderacao_aceita, motivo_negativa) 
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                               id, ano, mes, data_reclamacao, solucao_mes, nome_consumidor, descr_legenda, ocorrencia, produto, nivel, replic, resposta_publica, 
                               avaliacao, nota, voltaria_fazer_negocio, caso_resolvido, o_que_houve_no_atendimento, moderacao_solicitada, data_moderacao, motivo, 
                               moderacao_aceita, motivo_negativa)
        except Exception as e:
            print(f"Erro ao processar linha {index}: {e}")

    conn.commit()

def main():
    file_id = '1Kgomn2uhzQBnJQfO6J9Qhpyze7tT7RuNUk-1zLOx7zM'
    mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    destination_file_name = 'C:\\Users\\thorodin\\Desktop\\arquivo_baixado\\arquivo.xlsx'
    
    # Fazer o download do arquivo
    export_and_download_file(file_id, mime_type, destination_file_name)
    
    # Migrar a planilha
    migrate_sheet_to_xls(xlsx_filename=destination_file_name, sheet_name='Consolidado')
    
    # Carregar o arquivo .xls migrado
    file_path = destination_file_name.replace('.xlsx', '.xls')
    
    try:
        df = load_excel_data(file_path)
        print("Colunas do arquivo Excel:")
        print(df.columns)  # Imprime as colunas do DataFrame
        conn = connect_to_db()
        upsert_data(conn, df)
        print("Dados atualizados com sucesso!")
    except Exception as e:
        print(f"Erro ao processar dados: {e}")

if __name__ == '__main__':
    main()
