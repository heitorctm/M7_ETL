from auxiliares import (
    substituir_virgula_por_ponto,
    substituir_nan_por_zero,
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas,
)


def t_compromissada(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `compromissada`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Renomeia as colunas
    novo_nome_colunas = [
        "data_ref",
        "data_venc",
        "cod_aai",
        "cod_xp",
        "posicao",
        "taxa",
        "receita",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))

    # Realiza transformações específicas
    dados = substituir_virgula_por_ponto(
        dados, colunas=["taxa"]
    )  # Apenas para essa tabela
    dados = substituir_nan_por_zero(dados, coluna="posicao")
    dados = truncar_2_casas(dados, colunas=["receita", "posicao"])
    dados = substituir_nan_por_zero(dados, coluna="receita")
    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados)
    dados = adicionando_aspas_duplas(
        dados, colunas_not_varchar=["data_ref", "data_venc"]
    )

    return dados


def processar_tabela_compromissada(dados):
    """
    Função principal que processa os dados da tabela `compromissada`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_compromissada(dados)
    return dados_processados
