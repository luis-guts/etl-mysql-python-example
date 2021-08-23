import pymysql
import environment

def main():
    
    print("Starting job")

    hostname = environment.hostname
    username = environment.username
    password = environment.password
    database = environment.database

    myconnection = pymysql.connect(
        host = hostname,
        user = username,
        passwd = password,
        db = database
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
                        ID_LINHA,
                        LINHA,
                        year(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS ANO_VENDA,
                        month(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS MES_VENDA,
                        QTD_VENDA 
                        FROM vendas.historico_venda 
                        order by str_to_date(DATA_VENDA, "%d/%m/%Y") asc
                    ) AS HISTORICO_VENDA_FORMATADO
                group by HISTORICO_VENDA_FORMATADO.ID_LINHA, HISTORICO_VENDA_FORMATADO.LINHA, HISTORICO_VENDA_FORMATADO.ANO_VENDA, HISTORICO_VENDA_FORMATADO.MES_VENDA""")
    

        result = curs.fetchall()

        insert = """insert into linha_ano_mes(id_linha, linha, ano, mes, quantidade_vendida)
        VALUES(%s, %s, %s, %s, %s)
        """
        curs.executemany(insert, result)

        myconnection.commit()
        print(str(len(result)) + " Linhas inseridas")
        myconnection.close()

    return ("Done!", 200)

main()