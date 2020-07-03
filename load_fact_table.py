import apache_beam as beam
from beam_nuggets.io import relational_db

#configuracion de target
source_config = relational_db.SourceConfiguration(
    drivername='postgresql',
    host='localhost',
    port=5432,
    username='postgres',
    password='Admin5310$',
    database='conekta',
)

table_config = relational_db.TableConfiguration(
    name='charges',
    create_if_missing=True,
    primary_key_columns=['id']
)

#inicio apache beam
p = beam.Pipeline()


class PrepareData(beam.DoFn):
    def process(self, element):
        #proceso de id
        del element['_id']

        #elimino campo compania
        del element['name']

        #verifico los montos que sean flotantes
        try:
            element['amount'] = 0 if element['amount'] == '' else float(element['amount'])
            element['amount'] = round(element['amount'] , 6)
        except:
            print(element['amount'])
            element['amount'] = 0
    
        #renombro el camo de actualizacion
        element['updated_at'] = element['paid_at']
        if element['updated_at'] == '':
            element['updated_at'] = None
        del element['paid_at']

        #retorno async
        yield element



main = (
        p
        |'data source '>> beam.io.ReadFromMongoDB(uri='mongodb://localhost:27017'
                                                         ,db='conekta'
                                                         ,coll='data_stagin'
                                                         ,projection={'company_name': 1, 'company_id' : 1  })
)

prov =(
        main
        |'filtro por identificador de compania'>> beam.Filter(lambda row : len(row['company_id']) > 24) 
        |'prepara informacion' >> beam.ParDo(PrepareData())
        |'verificar que el monto no sea infinito' >> beam.Filter( lambda row : row['amount'] != float('inf'))
        | 'Writing to DB table' >> relational_db.Write(
                                                        source_config=source_config,
                                                        table_config=table_config

                                                    )
        
)

p.run().wait_until_finish()