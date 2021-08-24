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
        curs.execute("Truncate table vendas.marca_linha")
        myconnection.commit()

        curs.execute("""SELECT
                            ID_MARCA
                        ,	MARCA
                        ,   ID_LINHA
                        ,	LINHA
                        ,	SUM(QTD_VENDA) As QTD_VENDA
                        FROM vendas.historico_venda
                        group by ID_MARCA, MARCA, ID_LINHA, LINHA;""")
    
        getData = True

        while getData:
            result = curs.fetchmany(environment.chunksize)
            if len(result) != 0:
                insert = """insert into marca_linha(id_marca, marca, id_linha, linha, quantidade_vendida)
                VALUES(%s, %s, %s, %s, %s)
                """
                with myconnection.cursor() as insertCurs:
                    print("lote inserido")
                    insertCurs.executemany(insert, result)
            else:
                print("Fim da lista de chunks")
                getData = False

        myconnection.commit()
        print("Dados inseridos")
        myconnection.close()

    return ("Done!", 200)

main()