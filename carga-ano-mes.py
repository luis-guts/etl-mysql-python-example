import pymysql
import environment

def main():
    print("Starting job")

    myconnection = pymysql.connect(
        host = environment.hostname,
        user = environment.username,
        passwd = environment.password,
        db = environment.database
    )

    with myconnection.cursor() as curs:
        curs.execute("Truncate table vendas.ano_mes")
        myconnection.commit()

        curs.execute("""SELECT  CAST(HISTORICO_VENDA_FORMATADO.ANO_VENDA AS SIGNED) ano
        ,		CAST(HISTORICO_VENDA_FORMATADO.MES_VENDA AS SIGNED) mes
        , 	    CAST(SUM(HISTORICO_VENDA_FORMATADO.QTD_VENDA) AS SIGNED) AS quantidade_vendida
        FROM (
	        	SELECT 
		        year(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS ANO_VENDA,
		        month(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS MES_VENDA,
		        QTD_VENDA 
                FROM vendas.historico_venda 
                order by str_to_date(DATA_VENDA, "%d/%m/%Y") asc
	        ) AS HISTORICO_VENDA_FORMATADO
        group by HISTORICO_VENDA_FORMATADO.ANO_VENDA, HISTORICO_VENDA_FORMATADO.MES_VENDA""")
    
        getData = True

        while getData:
            result = curs.fetchmany(environment.chunksize)
            if len(result) != 0:
                
                print("inserindo lote")
                insert = """insert into ano_mes(ano, mes, quantidade_vendida)
                VALUES(%s, %s, %s)
                """
                with myconnection.cursor() as intserCurs:    
                    intserCurs.executemany(insert, result)
                myconnection.commit()
            else:
                print("fim da lista de chunks")
                getData = False


        print("Dados inseridos")
        myconnection.close()

    return ("Done!", 200)

main()