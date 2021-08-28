import pymysql
import environment
import traceback

def main(request):
    try:
        print("Starting job")

        myconnection = pymysql.connect(
            host = environment.hostname,
            user = environment.username,
            passwd = environment.password,
            db = environment.database
        )

        with myconnection.cursor() as curs:
            curs.execute("Truncate table vendas.linha_ano_mes")
            myconnection.commit()

            curs.execute("""SELECT
                            HISTORICO_VENDA_FORMATADO.ID_LINHA id_linha
                    ,       HISTORICO_VENDA_FORMATADO.LINHA linha
                    ,		HISTORICO_VENDA_FORMATADO.ANO_VENDA ano
                    ,		HISTORICO_VENDA_FORMATADO.MES_VENDA mes
                    , 	    SUM(HISTORICO_VENDA_FORMATADO.QTD_VENDA) AS quantidade_vendida
                    FROM (
                        SELECT
                        ID_LINHA id_linha,
                        LINHA linha,
                        year(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS ANO_VENDA,
                        month(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS MES_VENDA,
                        QTD_VENDA 
                        FROM vendas.historico_venda
                        order by str_to_date(DATA_VENDA, "%d/%m/%Y")
                        ) AS HISTORICO_VENDA_FORMATADO
                    group by HISTORICO_VENDA_FORMATADO.ID_LINHA, HISTORICO_VENDA_FORMATADO.LINHA, HISTORICO_VENDA_FORMATADO.ANO_VENDA, HISTORICO_VENDA_FORMATADO.MES_VENDA
                    order by id_linha, ano desc, mes asc
                    """)
        
            getData = True

            while getData:
                result = curs.fetchmany(50)
                if len(result) != 0:
                    insert = """insert into vendas.linha_ano_mes(id_linha, linha, ano, mes, quantidade_vendida)
                    VALUES(%s, %s, %s, %s, %s)
                    """
                    print("inserindo lote")
                    with myconnection.cursor() as insertCurs:
                        insertCurs.executemany(insert, result)
                else:
                    print("Fim da lista de chunks")
                    getData = False

            myconnection.commit()
            print("Dados inseridos")
            myconnection.close()

        return ("Done!", 200)
    
    except Exception:
        return (traceback.format_exc(), 500)

main(1)