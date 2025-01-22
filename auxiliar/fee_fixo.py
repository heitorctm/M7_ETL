from .auxiliares import (
    acessar_s3,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data_transf,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
    corrigir_datas_absurdas,
)


def t_fee_fixo_s3(dados, path_do_arquivo):

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
    dados = corrigir_datas_absurdas(dados, "data_contratacao")
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_contratacao"]
    )
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_contratacao"])
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_fee_fixo_s3(path_do_arquivo, nome_base="fee_fixo", bucket="m7investimentos"):

    pasta_destino = "../../BASE/12 - fee_fixo/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_fee_fixo_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
