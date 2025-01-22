from .auxiliares import (
    consulta_bc,
    acessar_s3,
    truncar_2_casas,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)
import pandas as pd


def t_xpus_cap_s3(dados, path_do_arquivo):

    nova_ordem_colunas = [
        "Date",
        "Código da Conta Offshore PWM",
        "Código da Conta Brasil",
        "Tipo de Operação",
        "Volume Financeiro Movimentado ($)",
        "Código do Assessor",
    ]

    dados = dados[nova_ordem_colunas]

    novo_nome_colunas = ["data_ref", "cod_us", "cod_xp", "tipo", "captacao", "cod_aai"]

    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))
    dados = remover_linhas_sem_data(dados)
    dados = formatar_colunas_data(dados, colunas_not_varchar=["data_ref"])
    dados["Fator BC"] = dados["data_ref"].apply(
        lambda date: consulta_bc(date.strftime("%d/%m/%Y"))
    )
    dados["captacao"] = pd.to_numeric(dados["captacao"], errors="coerce")
    dados["captacao"] = dados["captacao"] * dados["Fator BC"]
    dados = dados.drop(columns=["Fator BC"])
    dados = remover_letras_coluna(dados, coluna="cod_aai")
    dados = truncar_2_casas(dados, colunas=["captacao"])
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_xpus_cap_s3(path_do_arquivo, nome_base="xpus_cap", bucket="m7investimentos"):

    pasta_destino = "../../BASE/7 - xpus_cap/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_xpus_cap_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
