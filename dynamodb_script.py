from unicodedata import name
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('hosts')

def retorna_itens():
    '''função que retorna todos os itens da tabela'''
    response = table.scan()
    data = response['Items']
    return data

def insere_item(ip, name, local, login, senha):
    '''Função que inserem 1 item na tabela e retorna True or False se a operação
    foi correta ou não, respectivamente'''
    response = table.put_item(
        Item = {
            'ip': ip,
            'name': name,
            'local': local,
            'login': login,
            'senha': senha
        }
    )
    if response:
        return True
    else:
        return False

def retorna_item_ip(ip):
    '''retorna um item específico informando o ip'''
    response = table.query(
        KeyConditionExpression=Key('ip').eq(ip)        
    )
    items = response['Items']
    return items

def retorna_item_name(name):
    '''retorn um item específico informando o name'''
    response = table.scan(
        FilterExpression=Attr('name').eq(name)
    )
    items = response['Items']
    return items

def delete_item_ip(ip, name):
    '''deleta um item específico com base no ip e name informado (primary key, sortkey)'''
    response = table.delete_item(
        Key={
            'ip': ip,
            'name': name
        }
    )
    if response:
        return "item deletado"
    else:
        return "erro"