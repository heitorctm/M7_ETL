import pandas as pd
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError
import shutil
import subprocess
import time
import asyncio
import urllib.request
import json


def acessar_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=id_heitor,
            aws_secret_access_key=senha_heitor,
            region_name="sa-east-1",
        )
        return s3
    except ClientError as e:
        print(f"erro!!! {e}")
        return None


def inserir_arquivo_no_s3(nome_base, arquivo, s3, bucket="m7investimentos"):
    path_da_base = nome_base + "/"

    data_atual = datetime.now().strftime("%Y-%m-%d")
    s3_file_key = f"{path_da_base}{nome_base}_{data_atual}.csv"

    try:
        if s3 is None:
            raise ValueError("O cliente S3 nao foi inicializado.")

        s3.upload_file(arquivo, bucket, s3_file_key)
        print(f"{arquivo} enviado para {s3_file_key} no bucket {bucket}.")
    except FileNotFoundError:
        print(f"erro!!! {arquivo}")
    except ClientError as e:
        print(f"erro!!! {e}")
    except Exception as e:
        print(f"erro!!! {e}")


def mover_arquivos_para_pasta(s3, bucket, pasta_origem, pasta_destino):
    try:
        if s3 is None:
            raise ValueError("O cliente S3 não foi inicializado.")

        # Listando os arquivos na pasta de origem
        response = s3.list_objects_v2(Bucket=bucket, Prefix=pasta_origem)
        if "Contents" not in response:
            print(f"Nenhum arquivo encontrado na pasta {pasta_origem}.")
            return

        for item in response["Contents"]:
            arquivo_origem = item["Key"]
            if arquivo_origem.endswith("/"):  # Ignorar diretórios
                continue

            # Construindo o caminho do arquivo de destino
            arquivo_destino = f"{pasta_destino}/{arquivo_origem.split('/')[-1]}"

            # Copiar o arquivo para o destino
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": arquivo_origem},
                Key=arquivo_destino,
            )

            # Excluindo o arquivo original
            s3.delete_object(Bucket=bucket, Key=arquivo_origem)
            print(f"Arquivo {arquivo_origem} movido para {arquivo_destino}.")

        print("Todos os arquivos foram movidos com sucesso.")

    except ClientError as e:
        print(f"Erro ao mover arquivos: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


import os
import shutil  # Para mover os arquivos


def mover_xperformance_s3(pasta_local, bucket, pasta_destino, s3):
    try:
        if s3 is None:
            raise ValueError("O cliente S3 não foi inicializado.")

        pasta_processado = os.path.join(pasta_local, "processado")
        os.makedirs(pasta_processado, exist_ok=True)  # Cria a pasta se não existir

        cont = 0

        for arquivo in os.listdir(pasta_local):
            caminho_arquivo = os.path.join(pasta_local, arquivo)

            if os.path.isfile(caminho_arquivo):
                s3_file_key = f"{pasta_destino}/{arquivo}"

                s3.upload_file(caminho_arquivo, bucket, s3_file_key)
                cont += 1  # Incrementa o contador
                print(f"{caminho_arquivo} enviado. Arquivo {cont}")

                caminho_processado = os.path.join(pasta_processado, arquivo)
                shutil.move(caminho_arquivo, caminho_processado)

        print("Todos os arquivos foram movidos com sucesso.")
    except FileNotFoundError as e:
        print(f"Erro: Arquivo ou pasta local não encontrada. {e}")
    except ClientError as e:
        print(f"Erro ao enviar para o S3: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


def carregar_arquivo(path_do_arquivo):
    try:
        _, extensao = os.path.splitext(path_do_arquivo)

        if extensao.lower() == ".csv":
            df = pd.read_csv(path_do_arquivo, dtype=str, index_col=False)
        elif extensao.lower() == ".xlsx":
            df = pd.read_excel(
                path_do_arquivo, dtype=str, index_col=False, keep_default_na=False
            )
        else:
            raise ValueError("tipo de arquivo nao suportado!!")

        return df
    except FileNotFoundError:
        print(f"erro!!! {path_do_arquivo}")
        return None
    except ValueError as e:
        print(f"erro!!! {e}")
        return None
    except Exception as e:
        print(f"erro!!! {e}")
        return None


def adicionando_aspas_duplas(df, colunas_not_varchar):
    df = formatar_colunas_data(df, colunas_not_varchar)

    for col in df.columns:
        if col not in colunas_not_varchar:
            df[col] = df[col].apply(lambda x: f'"{x}"' if not pd.isna(x) else x)

    df.columns = [
        f'"{col}"' if col not in colunas_not_varchar else col for col in df.columns
    ]

    return df


def adicionando_aspas_duplas_positivador(df, colunas_not_varchar):
    df = formatar_colunas_data_positivador(df, colunas_not_varchar)

    for col in df.columns:
        if col not in colunas_not_varchar:
            df[col] = df[col].apply(lambda x: f'"{x}"' if not pd.isna(x) else x)

    df.columns = [
        f'"{col}"' if col not in colunas_not_varchar else col for col in df.columns
    ]

    return df


def deletar_4_ultimas_colunas(df):
    return df.iloc[:, :-4]


def corrigir_datas_absurdas(df, coluna_data):

    data_limite_minima = pd.Timestamp("1900-01-01")
    data_substituta = pd.Timestamp("2022-01-01")

    df[coluna_data] = pd.to_datetime(df[coluna_data], errors="coerce")

    df[coluna_data] = df[coluna_data].apply(
        lambda x: data_substituta if pd.isna(x) or x < data_limite_minima else x
    )

    if not pd.api.types.is_datetime64_any_dtype(df[coluna_data]):
        df[coluna_data] = data_substituta

    return df


def formatar_colunas_data(df, colunas_not_varchar=["data_ref"]):
    for col in colunas_not_varchar:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def formatar_colunas_data_transf(df, colunas_not_varchar):
    for col in colunas_not_varchar:
        if col in df.columns:
            # Remover as horas antes de qualquer conversão
            df[col] = df[col].str.split(" ").str[0]

            # Converter para o formato dd/mm/yyyy primeiro
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

            # Converter para o formato final yyyy-mm-dd
            df[col] = df[col].dt.strftime("%Y-%m-%d")

            # Substituir valores inválidos por None
            df[col] = df[col].where(df[col].notna(), None)
    return df


def formatar_colunas_data_positivador(df, colunas_not_varchar=["data_ref"]):
    for col in colunas_not_varchar:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col], format="%d/%m/%Y", errors="coerce"
            ).dt.strftime("%Y-%m-%d")
    return df


def deletar_colunas(df, colunas_para_deletar):
    if isinstance(colunas_para_deletar, str):
        colunas_para_deletar = [colunas_para_deletar]
    return df.drop(columns=colunas_para_deletar, errors="ignore")


def remover_linhas_sem_data(df, colunas=["data_ref"]):
    for col in colunas:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df.dropna(subset=colunas, inplace=True)

    return df


def remover_letras_coluna(df, coluna="cod_aai"):
    def filtrar_apenas_numeros(valor):
        return "".join(char for char in str(valor) if char.isdigit())

    df[coluna] = df[coluna].astype(str).apply(filtrar_apenas_numeros)
    return df


def substituir_virgula_por_ponto(df, colunas=["taxa"]):
    for coluna in colunas:
        df[coluna] = df[coluna].astype(str).str.replace(",", ".", regex=False)
    return df


def truncar_2_casas(df, colunas=["receita"]):
    try:
        for coluna in colunas:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
            df[coluna] = df[coluna].round(2)
        return df
    except KeyError as e:
        print(f"erro!!! {e}")
    except Exception as e:
        print(f"erro!!! {e}")


def substituir_nan_por_zero(df, coluna="receita"):

    df[coluna] = df[coluna].fillna(0)

    return df


def remover_linhas_nan(df, coluna):
    df[coluna] = df[coluna].replace(r"^\s*$", float("NaN"), regex=True)
    df = df.dropna(subset=[coluna])
    return df


def mover_arquivo(path, pasta_destino):
    try:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"arquivo nao encontrado: {path}")

        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)

        destino = os.path.join(pasta_destino, os.path.basename(path))
        shutil.move(path, destino)
    except FileNotFoundError as e:
        print(e)
    except PermissionError as e:
        print(f"erro!!! {e}")
    except Exception as e:
        print(f"erro!!! {e}")


def salvar_csv(df, path, encoding="iso-8859-1"):
    try:
        hora_atual = datetime.now().strftime("%H-%M")

        base, extensao = os.path.splitext(path)

        base += f"_{hora_atual}"

        if extensao.lower() == ".xlsx":
            novo_path = base + ".csv"
        else:
            novo_path = base + extensao

        df.to_csv(
            novo_path,
            index=False,
            date_format="%Y-%m-%d",
            encoding=encoding,
            quoting=3,
            doublequote=False,
            escapechar="\\",
            # na_rep="",
        )

        print(f"CSV salvo em {novo_path}!")
        return novo_path
    except PermissionError as e:
        print(f"erro!!! {e}")
    except Exception as e:
        print(f"erro!!! {e}")
    return novo_path


def consulta_bc(data):
    url = f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?formato=json&dataInicial={data}&dataFinal={data}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                json_data = json.loads(response.read().decode())
                if json_data:  # Verifica se o retorno contém dados
                    df = pd.DataFrame(json_data)
                    valor = df.iloc[0]["valor"]
                    return valor
                else:
                    print(f"Sem dados para a data {data}")
                    return None
            else:
                print(f"Erro na API para a data {data}: {response.status}")
                return None
    except Exception as e:
        print(f"Erro ao acessar a API para a data {data}: {e}")
        return None


def carga_positivador(caminho_arquivo):
    if os.path.exists(caminho_arquivo) and caminho_arquivo.endswith(".exe"):
        try:
            os.startfile(caminho_arquivo)
            print("carga positivador iniciada!!!")
        except Exception as e:
            print(f"erro!!! {e}")
    else:
        print("erro!!!")


def carga_base(caminho_arquivo, entrada):
    if os.path.exists(caminho_arquivo) and caminho_arquivo.endswith(".exe"):
        try:

            processo = subprocess.Popen(
                caminho_arquivo,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=True,
            )

            processo.stdin.write(entrada + "\n")
            processo.stdin.flush()

            print("Carga base iniciada! O programa está rodando em segundo plano.")
        except Exception as e:
            print(f"Erro ao executar o programa: {e}")
    else:
        print("Erro: Caminho inválido ou arquivo não é um .exe")


def remover_hifen(df, colunas):

    for coluna in colunas:
        if coluna in df.columns:
            df[coluna] = df[coluna].replace("-", "", regex=True)
    return df
