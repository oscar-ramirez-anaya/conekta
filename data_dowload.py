import apache_beam as beam
import pyarrow as pa
from datetime import datetime

p = beam.Pipeline()


#schemas
schema  = pa.schema(
        [
             ("id" ,pa.string())
            ,("company_name" ,pa.string())
            ,("company_id" ,pa.string())
            ,("amount" ,pa.float32())
            ,("status" ,pa.string())
            ,("created_at" ,pa.string())
            ,("updated_at" ,pa.string())
        ])


class PrepareData(beam.DoFn):
    def process(self, element):
        #proceso de id
        del element['_id']

        #renobro el campo de nombre de compania
        element['company_name'] = element['name']
        del element['name']
        if element['company_name'] =='':
            element['company_name']  = 'default_name_'+element['company_id']

        #verifico los montos que sean flotantes
        try:
            element['amount'] = 0 if element['amount'] == '' else float(element['amount'])
        except:
            print(element['amount'])
            element['amount'] = 0


        #renombro el camo de actualizacion
        element['updated_at'] = element['paid_at']
        del element['paid_at']

        #retorno async
        yield element


main = (
        p
        |'Leemos el data lake' >> beam.io.ReadFromMongoDB(uri='mongodb://localhost:27017'
                                                         ,db='conekta'
                                                         ,coll='data_stagin') 
        |'filtramos por los que no tienen id' >> beam.Filter(lambda row: row['id'] != '')
        |'preparamos los datos' >> beam.ParDo(PrepareData())
        |'Escribir proceso to parquet' >> beam.io.WriteToParquet(
                                                            file_path_prefix='./data/conekta'
                                                            ,schema=schema
                                                            )
)

p.run()
