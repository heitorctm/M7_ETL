from auxiliares import (
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
)
import pandas as pd
from datetime import date


def t_corretagem(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `corretagem`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """

    dados = pd.read_excel(dados, index_col=False, dtype=str)
    dados["data_ref"] = dados["Data"]

    nova_ordem_colunas = [
        "data_ref",
        "Conta",
        "Data",
        "BMF",
        "BOV",
        "Total",
        "Cod Matriz",
        "Cod A",
        "Tipo Corretagem",
        "Canal",
    ]

    dados = dados[nova_ordem_colunas]

    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados, coluna="Cod A")
    dados = truncar_2_casas(dados, colunas=["BOV", "Total"])
    dados = formatar_colunas_data(dados, colunas_not_varchar=["data_ref", "Data"])

    hoje = date.today().strftime("%Y-%m.%d")

    dados.to_csv(
        f"C:/Users/Administrador/Documents/atualizacao_diaria/IMPORT_S3_SQL/BASE/1 - corretagem/{hoje}-corretagem.csv",
        index=False,
    )

    return dados
