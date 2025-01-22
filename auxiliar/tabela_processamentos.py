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
    # Mapeia as colunas originais para os novos nomes
    mapeamento_colunas = {
        "Status": "status",
        "Código Assessor Origem": "cod_aai",
        "Código Assessor Destino": "cod_aai_destino",
        "Data Solicitação": "data_solicitacao",
        "Data Transferência": "data_transferencia",
        "Código do Cliente": "cod_xp",
    }

    # Filtra as colunas necessárias
    dados = dados[list(mapeamento_colunas.keys())]

    # Renomeia as colunas usando o mapeamento
    dados = dados.rename(columns=mapeamento_colunas)

    # # Reorganiza na ordem desejada
    # nova_ordem = [
    #     "cod_xp",
    #     "cod_aai",
    #     "cod_aai_destino",
    #     "data_solicitacao",
    #     "data_transferencia",
    #     "status",
    # ]
    # dados = dados[nova_ordem]

    # Remove hifens
    dados = remover_hifen(dados, ["cod_aai"])

    # Formata as colunas de data
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_transferencia"]
    )
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_solicitacao"]
    )

    return dados

