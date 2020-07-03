import logging
import apache_beam as beam
import argparse


#configuracion de argumentos
parser = argparse.ArgumentParser(description='Procesos de ingesta de Datos')
parser.add_argument('--filename', dest='filename', 
                    help='nombre de archivo a cargar')
args = parser.parse_args()
filename = args.filename

#inicializo el pipeline
p = beam.Pipeline()

#creamos proceso declarativo de split
class Part(beam.DoFn):
    def process(self, element):
        return [element.replace('\r','').split(',')]

#creamos proceso declarativo de parseo
class Construc(beam.DoFn):
    def process(self, element):
        yield { 'id': str(element[0]) 
                ,'name':element[1] 
                ,'company_id':	element[2] 
                ,'amount':element[3] 
                ,'status':element[4] 
                ,'created_at':element[5] 
                ,'paid_at':element[6] 
                } 

#se define el pipeline
main = (
        p
        |'lectura del archivo' >> beam.io.ReadFromText(filename ,skip_header_lines=1)
        |'Seccionamos las lineas'>> beam.ParDo(Part())
        |'construye dict err'>> beam.ParDo(Construc())
        |'insercion de error en mongo'>>beam.io.WriteToMongoDB(
                                                                uri='mongodb://localhost:27017'
                                                               ,db='conekta'
                                                               ,coll='data_stagin',
                                                                )
        )

#ejecucion
p.run().wait_until_finish()