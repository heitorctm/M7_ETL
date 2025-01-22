from auxiliares import (
    salvar_csv,
    remover_hifen,
    adicionando_aspas_duplas,
    formatar_colunas_data_transf,
)


def t_tabela_processamentos(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `processamentos`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Remove as colunas indesejadas
    print(1)
    print(dados.head(1))
    colunas_a_remover = [
        "Nome Assessor Origem",
        "Nome Assessor Destino",
        "Origem Solicitação",
        "Código Solicitação",
    ]
    dados = dados.drop(columns=colunas_a_remover)
    print(2)
    print(dados.head(1))
    # Renomeia as colunas
    novos_nomes = [
        "status",
        "cod_xp",
        "cod_aai",
        "cod_aai_destinho",
        "data_solicitacao",
        "data_transferencia"        
    ]
    dados.columns = novos_nomes
    print(3)
    print(dados.head(1))

    # Formata as colunas de data
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_solicitacao"]
    )
    print(4)
    print(dados.head(1))
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_transferencia"]
    )
    print(5)
    print(dados.head(1))

    # Remove hífens e adiciona aspas duplas
    dados = remover_hifen(dados, ["cod_aai", "cod_aai_destinho"])
    print(6)
    print(dados.head(1))
    dados = adicionando_aspas_duplas(dados, ["none"])
    print(7)
    print(dados.head(1))
    return dados


def processar_tabela_processamentos(dados):
    """
    Função principal que processa os dados da tabela `processamentos`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_tabela_processamentos(dados)
    return dados_processados
