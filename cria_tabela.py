import logging
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

class Movie:
    def __init__(self, dyn_resources):
        """
        :param dyn_resources: A Boto3 Dynamo resource.
        """
        self.dyn_resources = dyn_resources
        self.table = None
        
    def create_table(self, table_name):
        """
        Cria uma tabela no DynamoDb que serve para guardar dados de um filme.
        A tabela utiliza o ano do filme como partition key e o titulo como sort key.
        
        :param table_name: O nome da tabela que será criada.
        :return: A tabela recem criada
        """
        try:
            self.table = self.dyn_resources.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'year', 'KeyType': 'HASH'}, #Partition Key
                    {'AttributeName': 'title', 'KeyType': 'RANGE'} #Sort Key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'year', 'AttributeType': 'N'},
                    {'AttributeName': 'title', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Não foi possível criar a tabela %s, Motivo: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error', 'Message'])
            raise
        else:
            return self.table
            