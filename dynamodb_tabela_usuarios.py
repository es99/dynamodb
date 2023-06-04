import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr


logger = logging.getLogger(__name__)
recurso = boto3.resource('dynamodb')

class Usuarios:
    
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = self.dyn_resource.Table('usuarios')
        
    def get_usuario(self, cpf):
        """
        Retorna dados de um único usuário a partir do seu cpf.
        :param cpf: O cpf do usuario
        """
        try:
            response = self.table.get_item(Key={'cpf': cpf})
        except ClientError as err:
            logger.error(
                "Erro ao retornar %s da tabela %s. Motivo: %s: %s",
                cpf, self.table.name,
                err.response['Error']['Code'],
                err.response['Error']['Message']
            )
            raise
        else:
            return response['Item']
        
    def delete_usuario(self, cpf):
        """
        Deleta um usuário da tabela
        
        :param cpf: O cpf do usuário a ser deletado
        """
        try:
            self.table.delete_item(Key={'cpf': cpf})
        except ClientError as err:
            logger.error(
                "Erro ao deletar o usuário %s. Motivo %s: %s",
                cpf, err.response['Error']['Code'], err.response['Error']['Message']
            )
            raise
    
    def retorna_emails(self):
        """
        Retorna todos os emails cadastrados da tabela usuários da infopublic
        """
        try:
            response = self.table.scan(
                FilterExpression = Attr('email').exists()
            )
        except ClientError as err:
            logger.error(
                "Erro ao retornar emails. Motivo %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message']
            )
            raise
        else:
            return response['Items']