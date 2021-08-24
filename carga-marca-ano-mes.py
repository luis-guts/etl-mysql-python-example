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
        curs.execute("Truncate table vendas.marca_ano_mes")
        myconnection.commit()

        curs.execute("""SELECT  
                        HISTORICO_VENDA_FORMATADO.ID_MARCA id_marca
                ,		HISTORICO_VENDA_FORMATADO.Marca marca
                ,		HISTORICO_VENDA_FORMATADO.ANO_VENDA ano
                ,		HISTORICO_VENDA_FORMATADO.MES_VENDA mes
                ,  	    SUM(HISTORICO_VENDA_FORMATADO.QTD_VENDA) AS quantidade_vendida
                FROM (
                SELECT
                ID_MARCA,
                Marca,
                year(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS ANO_VENDA,
                month(str_to_date(DATA_VENDA, "%d/%m/%Y")) AS MES_VENDA,
                QTD_VENDA 
                FROM vendas.historico_venda 
                order by str_to_date(DATA_VENDA, "%d/%m/%Y") asc
            ) AS HISTORICO_VENDA_FORMATADO
            group by HISTORICO_VENDA_FORMATADO.ID_MARCA, HISTORICO_VENDA_FORMATADO.Marca, HISTORICO_VENDA_FORMATADO.ANO_VENDA, HISTORICO_VENDA_FORMATADO.MES_VENDA""")
    
        getData = True

        while getData:
            result = curs.fetchmany(environment.chunksize)
            if len(result) != 0:
                insert = """insert into marca_ano_mes(id_marca, marca, ano, mes, quantidade_vendida)
                VALUES(%s, %s, %s, %s, %s)
                """
                with myconnection.cursor() as insertCurs:
                    insertCurs.executemany(insert, result)
            else:
                print("Fim da lista de chunks")
                getData = False

        myconnection.commit()
        print("Dados inseridos")
        myconnection.close()

    return ("Done!", 200)

main()