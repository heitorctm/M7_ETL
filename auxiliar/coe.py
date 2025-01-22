from .auxiliares import truncar_2_casas, acessar_s3, mover_arquivo, remover_linhas_sem_data, remover_letras_coluna, formatar_colunas_data, adicionando_aspas_duplas,salvar_csv, carregar_arquivo, inserir_arquivo_no_s3

def t_coe_s3(dados, path_do_arquivo):

    dados['data_ref'] = dados['Data Reserva']

    nova_ordem_colunas = ["data_ref","Cod. Cliente","Cod. Assessor","Captação",
        "Receita","Status","COE","DIE","Estrutura","Categoria","Mês","Data Reserva",
        "Fim Reservas","Data Liquidação","Data Vencimento","Cod. Escritorio"
        ]
    dados = dados[nova_ordem_colunas]
    
    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados, coluna='Cod. Assessor')
    dados = truncar_2_casas(dados,colunas=['Receita'])
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=['data_ref', 'Mês', 'Data Reserva', 'Data Liquidação', 'Data Vencimento'])
    path = salvar_csv(dados, path_do_arquivo)

    return path

def load_coe_s3(path_do_arquivo, nome_base='coe', bucket='m7investimentos'):

    pasta_destino='../../BASE/5 - coe/processado/'
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print('nao tem o arquivo')
        return True
    path = t_coe_s3(dados, path_do_arquivo)
    #mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    #mover_arquivo(path=path, pasta_destino=pasta_destino)


