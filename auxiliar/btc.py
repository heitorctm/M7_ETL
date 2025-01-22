from auxiliares import (
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas,
)


def t_btc(dados):
    """
    Função que realiza as transformações específicas no DataFrame de BTC.
    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Adiciona a coluna 'data_ref' com base na coluna 'Data'
    dados["data_ref"] = dados["Data"]

    # Define a nova ordem das colunas
    nova_ordem_colunas = [
        "data_ref",
        "Cliente",
        "Data",
        "Ativo",
        "Abertura",
        "Vencimento",
        "Tipo",
        "Qtd",
        "Preço",
        "Financeiro",
        "Receita",
        "Cod Matriz",
        "Cod A",
    ]
    dados = dados[nova_ordem_colunas]

    # Aplica as transformações
    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados, coluna="Cod A")
    dados = adicionando_aspas_duplas(
        dados, colunas_not_varchar=["data_ref", "Data", "Abertura", "Vencimento"]
    )

    return dados


def processar_tabela_btc(dados):
    """
    Função principal que processa os dados da tabela BTC.
    :param dados: DataFrame recebido diretamente.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_btc(dados)
    return dados_processados
