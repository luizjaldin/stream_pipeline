from kafka.gerador_menssagens_kafka import processador_de_vendas

if __name__ == "__main__":
    processador = processador_de_vendas()
    processador.run()