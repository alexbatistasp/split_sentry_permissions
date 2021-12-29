import sys
import json

    
def read_json(filename):
    with open('./' + filename) as f:
        data = json.load(f)
        return data
def escreve_novo_json(new_file,numero_file):    
    print("======escrevendo os itens no arquivo =====>" + 'permission'+str(numero_file)+'.json')        
    with open('permission'+str(numero_file)+'.json', 'w') as f:
	    json.dump(new_file, f)
def novo_dict():
    ##print("======carregando uma nova estrutura=====")        
    new_file = {
        "metaData": {
            "sentryVersion": "2.1.0",
            "sourceClusterName": "cluster1",
            "serviceName": "[HIVE]",
            "exportVersion": 2.0,
            "roleBasedPermissions": "false",
            "hdfsPrefixes":[],
            "hiveServiceUser": "hive",
            "hiveGroupName": "hive"
        },
        "dbPolicies": [],
        "roleGroupMapping": {},
        "kafkaPolicies": []
    }
    return new_file


##MAIN

print("script:", sys.argv[0])
print("numero de argumentos:", len(sys.argv))
print("Valores:" , str(sys.argv))
print("======START=====")

filename = str(sys.argv[1])
qtd_por_file = str(sys.argv[2])
print("Quantidade de itens por arquivo ==> " +str(qtd_por_file))

##lendo o arquivo de referencia
print("Lendo o arquivo a ser processado ==> " +filename)
parsed = read_json(filename)

print("Total de itens a trabalhar ==> " + str(len(parsed['dbPolicies'])))
##print("====antes====")
##print(json.dumps(new_file))    
contador = 0
numero_file = 1
new_file = novo_dict()
while contador <  len(parsed['dbPolicies']):
    to_print =  str(parsed['dbPolicies'][contador]['resource'])
    print("Trabalhando no item ==> " + to_print )
    new_file['dbPolicies'].append(parsed['dbPolicies'][contador])        
    if (int(len(new_file['dbPolicies'])) == int(qtd_por_file)):        
        escreve_novo_json(new_file,str(numero_file))        
        numero_file+=1
        new_file = novo_dict()
    contador+=1
escreve_novo_json(new_file,"final")
##print("====depois====")
##print(json.dumps(new_file))
