from auxiliares import (
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data_transf,
    adicionando_aspas_duplas,
    corrigir_datas_absurdas,
)


def t_fee_fixo(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `fee_fixo`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Renomeia as colunas
    novo_nome_colunas = [
        "cod_xp",
        "nome_cliente",
        "data_contratacao",
        "taxa",
        "excecao_rv",
        "resgate_fundo",
        "cod_aai",
        "status",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))

    # Aplica as transformações
    dados = corrigir_datas_absurdas(dados, "data_contratacao")
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_contratacao"]
    )
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_contratacao"])

    return dados


def processar_tabela_fee_fixo(dados):
    """
    Função principal que processa os dados da tabela `fee_fixo`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_fee_fixo(dados)
    return dados_processados
