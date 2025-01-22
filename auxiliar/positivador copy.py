from auxiliares import (
    truncar_2_casas,
    acessar_s3,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas_positivador,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)


def t_positivador_s3(dados, path_do_arquivo):

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
    dados = adicionando_aspas_duplas_positivador(
        dados,
        colunas_not_varchar=["Data Posição", "Data de Cadastro", "Data de Nascimento"],
    )
    dados = dados.rename(columns={"Data Posição": "data_ref"})

    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_positivador_s3(
    path_do_arquivo, nome_base="positivador", bucket="m7investimentos"
):

    pasta_destino = "../../BASE/0 - positivador/processado/"
    # dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_positivador_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
