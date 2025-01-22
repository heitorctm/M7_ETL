from .auxiliares import (
    acessar_s3,
    remover_linhas_nan,
    truncar_2_casas,
    mover_arquivo,
    remover_linhas_sem_data,
    formatar_colunas_data,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)


def t_cambio_s3(dados, path_do_arquivo):

    colunas = [
        "Data",
        "Código Cliente",
        "Assessor",
        "Lado Cliente",
        "Moeda",
        "Valor em Moeda Estrangeira",
        "Taxa Custo",
        "Taxa Cliente",
        "Valor em Reais",
        "Spread_Medio",
        "Receita a dividir",
        "DT_MN_CLI",
        "DT_ME_CLI",
    ]

    dados = dados[colunas]

    colunas_nova_ordem = [
        "data_ref",
        "cod_xp",
        "cod_aai",
        "tipo",
        "Moeda",
        "Valor em Moeda Estrangeira",
        "Taxa Custo",
        "Taxa Cliente",
        "Valor em Reais",
        "Spread_Medio",
        "Receita a dividir",
        "DT_MN_CLI",
        "DT_ME_CLI",
    ]

    dados = dados.rename(columns=dict(zip(dados.columns, colunas_nova_ordem)))
    dados = remover_linhas_nan(dados, coluna="cod_xp")

    dados = formatar_colunas_data(
        dados, colunas_not_varchar=["data_ref", "DT_MN_CLI", "DT_ME_CLI"]
    )
    dados = remover_linhas_sem_data(dados)
    dados = truncar_2_casas(
        df=dados,
        colunas=[
            "Valor em Moeda Estrangeira",
            "Taxa Custo",
            "Taxa Cliente",
            "Valor em Reais",
            "Spread_Medio",
            "Receita a dividir",
        ],
    )
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_cambio_s3(path_do_arquivo, nome_base="cambio_att", bucket="m7investimentos"):
    pasta_destino = "../../BASE/4 - cambio/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_cambio_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
