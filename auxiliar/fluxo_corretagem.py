from auxiliares import (
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas,
)


def t_fluxo_corretagem(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `fluxo_corretagem`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Define as colunas na nova ordem
    colunas_nova_ordem = [
        "Data",
        "Cod A",
        "Conta",
        "Suitability",
        "Matriz",
        "Ativo",
        "Qtd",
        "Corretagem",
        "Volume Negociado",
        "Produto",
        "Canal",
        "Tipo de Corretagem",
        "Mercado",
        "Lado",
    ]
    dados = dados[colunas_nova_ordem]

    # Renomeia as colunas
    novo_nome_colunas = [
        "data_ref",
        "cod_aai",
        "cod_xp",
        "suitability",
        "matriz",
        "ativo",
        "qtd",
        "receita",
        "volume",
        "produto",
        "canal",
        "tipo",
        "mercado",
        "lado",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))

    # Aplica as transformações
    dados = remover_linhas_sem_data(dados, colunas=["data_ref"])
    dados = remover_letras_coluna(dados)
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])

    return dados


def processar_tabela_fluxo_corretagem(dados):
    """
    Função principal que processa os dados da tabela `fluxo_corretagem`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_fluxo_corretagem(dados)
    return dados_processados
