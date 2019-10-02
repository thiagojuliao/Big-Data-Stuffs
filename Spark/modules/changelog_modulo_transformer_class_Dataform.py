
(a)| ds_consumer_name           | consumer_name                 | -
(b)| ds_first_name              | consumer_name (none)          | col("consumer_name").split(" ")[0]
(c)| ds_consumer_lastname       | ds_consumer_name (none)       | col("ds_consumer_name").split(" ")[-1]   
(d)| ds_consumer_lastname_upper | ds_consumer_lastname (none)   | upper(col("ds_consumer_lastname"))

(a) {
    "nome": "ds_consumer_name",
    "origem": "consumer_name"
} -> este renomeia a variável

(b) {
    "nome": "ds_first_name",
    "origem": None
} -> variável de grau 0

(c) {
    "nome": "ds_consumer_lastname".
    "origem": None
} -> variável de grau 1

(d) {
    "nome": "ds_consumer_lastname_upper",
    "origem": None
} -> variável de grau 2


<< SOLUÇÃO >>

(1) Listar todas as variáveis tais que "origem" = None;

infos_sem_origem = [info for info in self.Metadata if not info["origem"]]


(2) Dentre as variáveis listadas separar em dois grupos:
    (G0) Grupo das variáveis de grau 0
    (G1+) Grupo das variáveis de grau >= 1
    
infos_de_grau_maiorigual_um = []
    
for info in infos_sem_origem:
    try:
        << executa os módulos >>
    except:
        infos_de_grau_maiorigual_um.append(info)


(3) Executar os módulos para as variáveis tais que "origem" != None;

infos_com_origem = [info for info in self.Metadata if info["origem"]]

for info in infos_com_origem:
    << executa os módulos >>
    

(4) Executar os módulos para as variáveis de grau maior ou iguais a 1;

index = 0

while infos_de_grau_maiorigual_um:

    info = infos_de_grau_maiorigual_um[index]
    
    try:
        << executa os módulos >>
        infos_de_grau_maiorigual_um.remove(info)
        index = 0
     except:
        index += 1
