from auxiliares import (
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas,
)


def t_coe(dados):
    """
    Função que realiza as transformações específicas no DataFrame de COE.
    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Adiciona a coluna 'data_ref' com base na coluna 'Data Reserva'
    dados["data_ref"] = dados["Data Reserva"]

    # Define a nova ordem das colunas
    nova_ordem_colunas = [
        "data_ref",
        "Cod. Cliente",
        "Cod. Assessor",
        "Captação",
        "Receita",
        "Status",
        "COE",
        "DIE",
        "Estrutura",
        "Categoria",
        "Mês",
        "Data Reserva",
        "Fim Reservas",
        "Data Liquidação",
        "Data Vencimento",
        "Cod. Escritorio",
    ]
    dados = dados[nova_ordem_colunas]

    # Aplica as transformações no DataFrame
    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados, coluna="Cod. Assessor")
    dados = truncar_2_casas(dados, colunas=["Receita"])
    dados = adicionando_aspas_duplas(
        dados,
        colunas_not_varchar=[
            "data_ref",
            "Mês",
            "Data Reserva",
            "Data Liquidação",
            "Data Vencimento",
        ],
    )

    return dados


def processar_tabela_coe(dados):
    """
    Função principal que processa os dados da tabela de COE.
    :param dados: DataFrame recebido diretamente.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_coe(dados)
    return dados_processados
