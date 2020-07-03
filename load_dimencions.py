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
    name='companies',
    create_if_missing=True,
    primary_key_columns=['id']
)


p = beam.Pipeline()

def imprime(row):
    print(row)
    return row

class PrepareDataProv(beam.DoFn):
    def process(self, element):

        #renobro el campo de nombre de compania
        element['company_name'] = element['name']
        del element['name']
        if element['company_name'] =='':
            element['company_name']  = 'default-name-'+element['company_id']
        #retorno async
        yield (element['company_name'] +'_'+element['company_id'])

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
        |'prepara informacion' >> beam.ParDo(PrepareDataProv())
        |'agrupo por proveedor' >> beam.combiners.Count().PerElement()
        |'split de campos unicos' >> beam.Map(lambda row : row[0].split('_'))
        |'preparamos el registro' >> beam.Map(lambda row : {'id':row[1] , 'company_name' : row[0]})
        |'imprime' >> beam.Map(imprime) 
        | 'Writing to DB table' >> relational_db.Write(
                                                        source_config=source_config,
                                                        table_config=table_config
                                                    )
        
)

p.run().wait_until_finish()