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
                result = curs.fetchmany(50)
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
    
    except Exception:
        return (traceback.format_exc(), 500)

print (main(1))