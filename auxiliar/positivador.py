from auxiliar.auxiliares import (
    truncar_2_casas,
    remover_letras_coluna,
    formatar_colunas_data_positivador,
)


def t_positivador_s3(dados):
    # Reorganiza as colunas na ordem desejada
    nova_ordem_colunas = [
        "Data Posição",
        "Assessor",
        "Cliente",
        "Profissão",
        "Sexo",
        "Segmento",
        "Data de Cadastro",
        "Fez Segundo Aporte?",
        "Data de Nascimento",
        "Status",
        "Ativou em M?",
        "Evadiu em M?",
        "Operou Bolsa?",
        "Operou Fundo?",
        "Operou Renda Fixa?",
        "Aplicação Financeira Declarada Ajustada",
        "Receita no Mês",
        "Receita Bovespa",
        "Receita Futuros",
        "Receita RF Bancários",
        "Receita RF Privados",
        "Receita RF Públicos",
        "Captação Bruta em M",
        "Resgate em M",
        "Captação Líquida em M",
        "Captação TED",
        "Captação ST",
        "Captação OTA",
        "Captação RF",
        "Captação TD",
        "Captação PREV",
        "Net em M 1",
        "Net Em M",
        "Net Renda Fixa",
        "Net Fundos Imobiliários",
        "Net Renda Variável",
        "Net Fundos",
        "Net Financeiro",
        "Net Previdência",
        "Net Outros",
        "Receita Aluguel",
        "Receita Complemento Pacote Corretagem",
    ]
    dados = dados[nova_ordem_colunas]

    # Processa o DataFrame
    dados = remover_letras_coluna(dados, coluna="Assessor")
    dados = truncar_2_casas(
        dados,
        colunas=[
            "Aplicação Financeira Declarada Ajustada",
            "Receita Bovespa",
            "Captação Bruta em M",
            "Captação TED",
            "Net em M 1",
            "Net Em M",
            "Net Renda Fixa",
            "Net Fundos Imobiliários",
            "Net Renda Variável",
            "Net Fundos",
            "Net Financeiro",
            "Net Previdência",
            "Net Outros",
            "Receita Aluguel",
        ],
    )
    dados = formatar_colunas_data_positivador(
        dados,
        colunas_not_varchar=["Data Posição", "Data de Cadastro", "Data de Nascimento"],
    )
    dados = dados.rename(columns={"Data Posição": "data_ref"})
    return dados


def processar_tabela_positivador(dados):
    """
    Função principal que processa os dados da tabela.
    :param dados: DataFrame recebido diretamente do nó KNIME
    :return: DataFrame processado
    """
    # Chama a função de processamento principal
    dados_processados = t_positivador_s3(dados)
    return dados_processados
