from auxiliares import (
    remover_linhas_nan,
    truncar_2_casas,
    remover_linhas_sem_data,
    formatar_colunas_data,
    adicionando_aspas_duplas,
)


def t_cambio_att(dados):
    """
    Função que realiza as transformações específicas no DataFrame de câmbio.
    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Define as colunas utilizadas no processamento
    colunas = [
        "Data",
        "Código Cliente",
        "Assessor",
        "Lado Cliente",
        "Moeda",
        "Valor em Moeda Estrangeira",
        "Taxa Custo",
        "Taxa Cliente",
        "Valor em Reais",
        "Spread_Medio",
        "Receita a dividir",
        "DT_MN_CLI",
        "DT_ME_CLI",
    ]
    dados = dados[colunas]

    # Define a nova ordem das colunas e renomeia
    colunas_nova_ordem = [
        "data_ref",
        "cod_xp",
        "cod_aai",
        "tipo",
        "Moeda",
        "Valor em Moeda Estrangeira",
        "Taxa Custo",
        "Taxa Cliente",
        "Valor em Reais",
        "Spread_Medio",
        "Receita a dividir",
        "DT_MN_CLI",
        "DT_ME_CLI",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, colunas_nova_ordem)))

    # Aplica as transformações no DataFrame
    dados = remover_linhas_nan(dados, coluna="cod_xp")
    dados = formatar_colunas_data(
        dados, colunas_not_varchar=["data_ref", "DT_MN_CLI", "DT_ME_CLI"]
    )
    dados = remover_linhas_sem_data(dados)
    dados = truncar_2_casas(
        df=dados,
        colunas=[
            "Valor em Moeda Estrangeira",
            "Taxa Custo",
            "Taxa Cliente",
            "Valor em Reais",
            "Spread_Medio",
            "Receita a dividir",
        ],
    )
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])

    return dados


def processar_tabela_cambio_att(dados):
    """
    Função principal que processa os dados da tabela de câmbio.
    :param dados: DataFrame recebido diretamente.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_cambio_att(dados)
    return dados_processados
