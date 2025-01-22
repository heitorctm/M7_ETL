from auxiliares import (
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
)


def t_prod_estruturados(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `prod_estruturados`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Adiciona a coluna de referência de data
    dados["data_ref"] = dados["Data"]

    # Define a nova ordem de colunas
    nova_ordem_colunas = [
        "data_ref",
        "Cliente",
        "Data",
        "Origem",
        "Ativo",
        "Estratégia",
        "Comissão",
        "Cod Matriz",
        "Cod A",
        "Status da Operação",
    ]
    dados = dados[nova_ordem_colunas]

    # Aplica as transformações
    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados, coluna="Cod A")
    dados = formatar_colunas_data(dados, colunas_not_varchar=["data_ref", "Data"])
    dados = truncar_2_casas(dados, colunas=["Comissão"])
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])

    return dados


def processar_tabela_prod_estruturados(dados):
    """
    Função principal que processa os dados da tabela `prod_estruturados`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_prod_estruturados(dados)
    return dados_processados
