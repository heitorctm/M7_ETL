from .auxiliares import (
    acessar_s3,
    truncar_2_casas,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)


def t_prod_estruturados_s3(dados, path_do_arquivo):

    dados["data_ref"] = dados["Data"]

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

    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados, coluna="Cod A")
    dados = formatar_colunas_data(dados, colunas_not_varchar=["data_ref", "Data"])
    dados = truncar_2_casas(dados, colunas=["Comissão"])
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_prod_estruturados_s3(
    path_do_arquivo, nome_base="produtos_estruturados", bucket="m7investimentos"
):

    pasta_destino = "../../BASE/3 - prod_estruturados/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_prod_estruturados_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
