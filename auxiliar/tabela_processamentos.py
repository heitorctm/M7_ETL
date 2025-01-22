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

    colunas_a_ficar = [
        "Status",
        "Código Assessor Origem",
        "Código Assessor Destino",
        
        "Data Solicitação",
        "Data Transferência",
        "Código do Cliente",
    ]
    dados = dados[colunas_a_ficar]
    print(dados.head(1))

    novos_nomes = [
        "status",
        "cod_xp",
        "cod_aai",
        "cod_aai_destino",
        "data_solicitacao",
        "data_transferencia"        
    ]
    
    dados.columns = novos_nomes
    dados = remover_hifen(dados, ["cod_aai"])
    nova_ordem = [
        "cod_xp",
        "cod_aai",
        "cod_aai_destino",
        "data_solicitacao",
        "data_transferencia",
        "status"   
    ]
    dados = dados[nova_ordem]



    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_transferencia"]
    )

    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_solicitacao"]
    )

    dados["data_solicitacao"] = dados["data_solicitacao"].astype(str)


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
