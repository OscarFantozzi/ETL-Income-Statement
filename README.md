# ETL DRE (Demostração Resultados do Exercício)

[Overview do Projeto [1](#overview-do-projeto)](#overview-do-projeto)

[O que é uma DRE? [2](#o-que-é-uma-dre)](#o-que-é-uma-dre)

[Principais etapas de um ETL
[3](#principais-etapas-de-um-etl)](#principais-etapas-de-um-etl)

[Conhecendo os Dados do Projeto
[3](#conhecendo-os-dados-do-projeto)](#conhecendo-os-dados-do-projeto)

[Criando o ambiente virtual
[5](#criando-o-ambiente-virtual)](#criando-o-ambiente-virtual)

[Instalando as bibliotecas necessárias
[9](#instalando-as-bibliotecas-necessárias)](#instalando-as-bibliotecas-necessárias)

[Configurando o Jupyter Notebook
[10](#configurando-o-jupyter-notebook)](#configurando-o-jupyter-notebook)

[Configurando o Jupyter Lab
[12](#configurando-o-jupyter-lab)](#configurando-o-jupyter-lab)

[Conectando no Banco de Dados SQL Server
[15](#conectando-no-banco-de-dados-sql-server)](#conectando-no-banco-de-dados-sql-server)

[Primeira Etapa do ETL: Extract
[15](#primeira-etapa-do-etl-extract)](#primeira-etapa-do-etl-extract)

[Segunda Etapa do ETL: Transform
[20](#segunda-etapa-do-etl-transform)](#segunda-etapa-do-etl-transform)

[Terceira Etapa do ETL: Load
[34](#terceira-etapa-do-etl-load)](#terceira-etapa-do-etl-load)

[Criando a classe ETL
[38](#criando-a-classe-etl)](#criando-a-classe-etl)

[Criando o arquivo ETL.py
[38](#criando-o-arquivo-etl.py)](#criando-o-arquivo-etl.py)

[Criando o arquivo requirements.txt
[40](#criando-o-arquivo-requirements.txt)](#criando-o-arquivo-requirements.txt)

[Automatizando o ETL com o agendador de tarefas do Windows
[41](#_Toc175081322)](#_Toc175081322)

# Overview do Projeto

> Muitas empresas, tanto no Brasil quanto ao redor do mundo, ainda estão
> presas a métodos manuais para extrair e processar dados de seus
> sistemas ERP. Imagine, por exemplo, a tarefa de criar um relatório
> anual: o usuário precisa extrair um relatório para cada mês, tratar os
> dados manualmente e, finalmente, consolidar 12 arquivos separados para
> gerar o relatório anual. Agora, multiplique isso pelos últimos 5 anos.
> O tempo e o esforço envolvidos são imensos, sem mencionar o risco de
> erros manuais.
>
> Para resolver esse desafio, desenvolvi um projeto que automatiza todo
> esse processo. Utilizando Python, criei um pipeline de ETL (Extração,
> Transformação e Carga) que, com apenas um clique, realiza todas as
> etapas necessárias para gerar uma DRE (Demonstração do Resultado do
> Exercício) a partir de relatórios em CSV.
>
> **Como Funciona:**

1.  **Extração:** O script carrega automaticamente os dados financeiros
    a partir dos arquivos CSV.

2.  **Transformação:** As regras de transformação são aplicadas para
    garantir que os dados estejam consistentes e prontos para análise.

3.  **Carga:** Os dados transformados são armazenados em um banco de
    dados relacional, centralizando as informações para fácil acesso e
    gestão.

4.  **Visualização:** Um dashboard é desenvolvido em Power BI para
    exibir os resultados.

> **Vantagens:**

-   **Gestão Centralizada e Segura:** O banco de dados facilita a
    manutenção e atualização das informações, garantindo integridade e
    segurança dos dados.

-   **Consultas Ágeis:** Com os dados estruturados, as consultas no
    Power BI são mais rápidas, evitando longo tempos de espera ao
    carregar os dados.

-   **Escalabilidade:** A solução é facilmente escalável, suportando o
    aumento no volume de dados sem comprometer o desempenho.

-   **Maior Eficiência Operacional:** A automação reduz o tempo e o
    esforço necessários para o tratamento dos dados, eliminando erros
    manuais e aumentando a produtividade.

# O que é uma DRE?

Quando falamos em gestão empresarial, a DRE (Demonstração do Resultado
do Exercício), ou *[Income Statement]{.underline}* em inglês, é um dos
relatórios mais cruciais. Afinal, toda empresa quer saber se suas
operações estão gerando os resultados esperados. Enquanto o fluxo de
caixa mostra quanto dinheiro a empresa tem em um determinado momento, a
DRE mede o desempenho operacional.

Existem duas formas principais de DRE: a Contábil e a Gerencial. Neste
projeto, vou focar na DRE Gerencial, mas antes, vamos entender a
principal diferença entre elas.

**DRE Contábil:**

-   **Cumprimento de Exigências Legais:** A DRE Contábil é projetada
    para atender às exigências fiscais e legais.

-   **Conformidade com Normas Contábeis:** Segue rigorosamente os
    princípios contábeis, garantindo a transparência e a precisão
    necessárias para stakeholders externos, como investidores e governo.

-   **Baseada em Partidas Dobradas:** Toda transação envolve um débito e
    um crédito. Por exemplo, uma venda pode resultar no crédito de
    receita e débito de caixa ou contas a receber, se a venda for a
    prazo.

**DRE Gerencial:**

-   **Foco na Gestão Interna:** Esta versão da DRE é destinada ao uso
    interno, especialmente para gestores que precisam tomar decisões
    rápidas e informadas.

-   **Análise Direcionada:** Enfatiza as análises específicas que
    auxiliam na gestão e na estratégia da empresa, sem a necessidade de
    seguir as normas de débito e crédito.

# Principais etapas de um ETL

ETL (Extract, Transform, Load) é um processo que envolve a extração de
dados de diversas fontes, a transformação desses dados para que fiquem
prontos para análise e, finalmente, a carga deles em um banco de dados
ou outro sistema de armazenamento.

No contexto deste projeto, as principais etapas seguem a estrutura
tradicional de um processo de ETL:

1.  **Carregamento dos Dados (Extract):**

    -   Leitura e armazenamento dos arquivos CSV em um banco de dados
        SQL Server.

2.  **Transformação dos Dados (Transform):**

    -   Implementação em Python das transformações e tratamentos que
        anteriormente eram realizados no Power Query.

3.  **Carga no Banco de Dados (Load):**

    -   Inserção dos dados transformados no banco de dados SQL Server,
        prontos para serem consumidos pelo Power BI.

# Tabelas Fato e Dimensão

Tabelas fato, são tabelas que armazenam o histórico de eventos do
negócio, como vendas ou transações, geralmente possuem uma coluna de
datas. Elas se conectam às tabelas dimensão através de **chaves
estrangeiras** (campo em uma tabela que se refere à chave primária de
outra tabela, criando uma ligação entre as duas tabelas).

As tabelas dimensão complementam as informações das tabelas fato,
fornecendo detalhes como nomes de produtos ou locais. Elas usam **chaves
primárias** (campo único em uma tabela que identifica de forma exclusiva
cada registro, garantindo que não haja duplicatas).

Juntas, essas tabelas permitem análises detalhadas, ligando o histórico
de eventos às suas descrições.

#  Conhecendo os Dados do Projeto

Vamos agora explorar brevemente os dados que utilizamos neste projeto.
Os arquivos exportados são organizados em uma pasta específica, contendo
dados relativos a três anos: 2022, 2023 e 2024. Cada arquivo segue uma
convenção de nomenclatura que facilita a identificação dos lançamentos
por ano e tipo de conta contábil. Por exemplo, o arquivo
fLancamento1_ano1 contém todos os lançamentos relacionados às contas
contábeis do tipo 01.xx.xx para o ano 2022, enquanto fLancamento2_ano2
se refere às contas do tipo 02.xx.xx para o ano 2023, e assim por
diante.02.xx.xx e assim por diante.

![](media/image1.png)

Para ilustrar melhor, aqui estão as principais tabelas utilizadas no
projeto:´

**Tabela: dEstruturaDRE**

**Tipo: Dimensão**

-   **id:** Identificador único que combina um código de grupo com o
    número da conta gerencial.

-   **index:** Índice numérico que define a ordem sequencial das contas
    gerenciais.

-   **contaGerencial:** Nome da conta gerencial que descreve tipos
    específicos de receitas, despesas ou resultados.

-   **subtotal:** Indicador que sinaliza se a linha representa um
    subtotal ou valor final de cálculo.

-   **empresa:** Nome da empresa ou localidade à qual os dados
    financeiros se referem.

**Tabela: dPlanoConta**

**Tipo: Dimensão**

-   **id:** Identificador único que combina o código hierárquico com o
    número da linha específica da descrição.

-   **index:** Índice numérico que define a ordem sequencial dos itens
    dentro da descrição.

-   **descricaoN1:** Descrição de nível 1 que especifica o tipo geral de
    transação ou categoria.

-   **descricaoN2:** Descrição de nível 2 que fornece um detalhamento
    adicional ou mais específico da transação.

-   **detalharN2:** Indicador binário que mostra se há um detalhamento
    adicional (0 para não detalhar, 1 para detalhar).

-   **mascaraDRE_id:** Referência ao identificador na máscara da DRE,
    associando a linha a uma categoria específica da DRE.

-   **tipoLancamento:** Tipo de lançamento que indica a natureza da
    transação, com valores como 1 para adição e -1 para subtração.

**Tabela: fOrcamento**

**Tipo: Fato**

-   **competencia_data:** Data de competência que indica o período a que
    a transação ou valor se refere.

-   **planoContas_id:** Identificador da conta no plano de contas, que
    associa o valor a uma categoria específica dentro da contabilidade.

-   **valor:** Valor monetário registrado para a transação ou conta
    específica na data de competência indicada.

**Tabela: fPrevisao**

**Tipo: Fato**

-   **competencia_data:** Data de competência que indica o período
    contábil ao qual os valores registrados se referem.

-   **planoContas_id:** Identificador da conta no plano de contas, que
    classifica a natureza da transação ou item contábil.

-   **valor:** Quantia monetária associada à conta específica na data de
    competência indicada.

**Tabelas: fLancamento1_ano1 / fLancamento2_ano1 / fLancamento3_ano1**

**Tipo: Fato**

-   **competencia_data:** Data de competência que indica o período
    contábil ao qual os valores registrados se referem.

-   **planoContas_id:** Identificador da conta no plano de contas, que
    classifica a natureza da transação ou item contábil.

-   **valor:** Quantia monetária associada à conta específica na data de
    competência indicada.

# Criando o ambiente virtual

Criar um ambiente virtual é essencial para isolar os pacotes usados em
um projeto, garantindo que as versões das bibliotecas sejam consistentes
e evitando o famoso problema de compatibilidade \"mas na minha máquina
funciona\". Ao final do projeto, é gerado um arquivo requirements.txt,
que lista todas as bibliotecas instaladas naquele ambiente. Isso permite
que o script seja executado em qualquer ambiente de forma consistente,
desde que o requirements.txt seja utilizado.

Para criar o ambiente virtual, vou usar o PowerShell do Windows. Aqui
estão os passos que sigo:

**Navego até a Pasta do Projeto:**

-   Primeiro, navego até a pasta onde meu projeto está localizado. Para
    isso, uso o comando cd seguido pelo caminho da pasta do projeto.

-   Em seguida, executo o seguinte comando para criar o ambiente virtual

-   Substituo \"nome_do_ambiente\" pelo nome que quero dar ao ambiente
    virtual.

![](media/image2.png){width="5.905555555555556in"
height="2.436111111111111in"}

Agora, todos os pacotes que eu instalar estarão isolados nesse ambiente.

**[Caso Enfrente Problemas com Execução de Comandos
Python:]{.underline}**

Se eu não conseguir executar comandos Python no PowerShell, pode ser
necessário adicionar o Python às variáveis de ambiente do Windows. Para
fazer isso:

-   Digito \"variáveis\" na barra de busca do Windows e seleciono
    \"Editar as variáveis de ambiente do sistema\".

![](media/image3.png){width="5.905555555555556in"
height="5.071527777777778in"}

-   Na janela que aparece, clico em \"Variáveis de ambiente\...\".

E a seguir em "Editar\..."

![](media/image4.png){width="4.6574978127734035in"
height="3.744403980752406in"}

Por último, clico em \"Novo\" e adiciono o caminho onde o Python está
instalado no meu computador

![](media/image5.png){width="4.547054899387576in"
height="4.978943569553806in"}

# Instalando as bibliotecas necessárias

Agora vou instalar as bibliotecas necessárias para realizar o processo
de ETL. Para isso, utilizo o Jupyter Notebook ou Jupyter Lab como IDE,
que são as telas onde escrevo e executo meus códigos. Além disso,
preciso do pandas, uma biblioteca poderosa para manipulação de dados, e
do pyodbc, que vou usar para me conectar a uma instância de SQL Server.
Conforme o projeto avança, vou instalando outras bibliotecas conforme
necessário.

Para instalar uso os comandos:

![](media/image6.png){width="5.905555555555556in"
height="1.0319444444444446in"}

![](media/image7.png){width="5.910571959755031in"
height="1.9564577865266841in"}

# Configurando o Jupyter Notebook

Uma IDE, ou *Integrated Development Environment* (Ambiente de
Desenvolvimento Integrado), é uma ferramenta que combina diferentes
recursos, como um editor de código, depurador e terminal, para facilitar
o desenvolvimento de software. No caso do Jupyter Notebook, a interface
web atua como a IDE onde escrevo e executo meus códigos.

Para abrir o Jupyter Notebook, uso o comando:

![](media/image8.png){width="5.905555555555556in"
height="1.1847222222222222in"}

Após a execução, o terminal me mostra duas URLs onde posso acessar o
Jupyter Notebook, além de abrir automaticamente um bloco de notas com a
URL padrão. Para abrir o Jupyter Notebook, normalmente preciso copiar
essa URL e colar no navegador.

![](media/image9.png){width="5.905555555555556in"
height="2.029166666666667in"}

![](media/image10.png){width="5.905555555555556in"
height="2.0618055555555554in"}

![](media/image11.png){width="5.905555555555556in"
height="3.2881944444444446in"}

No entanto, prefiro automatizar esse processo para que o Jupyter
Notebook abra diretamente no Google Chrome, que é o navegador que estou
usando. Para isso, no PowerShell, executo um comando para gerar o
arquivo de configurações do Jupyter. Esse arquivo é criado na pasta
.jupyter, que no meu caso está localizada em C:\\Users\\oscar\\.jupyter.

![](media/image12.png){width="5.905555555555556in"
height="1.5298611111111111in"}

Para editar o arquivo de configuração, abro-o com o Bloco de Notas.

![](media/image13.png){width="4.013838582677165in"
height="3.0190069991251094in"}

Uma vez aberto, uso Ctrl + F para procurar o seguinte texto:
c.ServerApp.browser.

![](media/image14.png){width="5.905555555555556in"
height="2.1791666666666667in"}

Em seguida, removo o comentário (símbolo #) e coloco entre aspas simples
(\' \') o caminho onde o Chrome está instalado na minha máquina. No meu
caso, o Chrome está em "C:\\Program Files\\Google\\Chrome.exe". Depois
disso, salvo o arquivo e executo o Jupyter Notebook novamente.

**[Atenção]{.underline}:** Ao copiar o caminho do Chrome, troquei as
barras invertidas (\\) por barras duplas (\\\\), ou troque por barras
normais (/). Além disso, adicione um espaço e \'%s\' após o caminho
entre aspas duplas. Por exemplo:

![](media/image15.png){width="5.905555555555556in" height="0.99375in"}

Além disso, verifiquei se o Chrome está nas variáveis de ambiente do
Windows (pois estou usando Windows).

Depois de seguir esses passos, o Jupyter Notebook abrirá automaticamente
no Google Chrome.

# Configurando o Jupyter Lab

Outra IDE amplamente utilizada é o Jupyter Lab. Ele oferece algumas
vantagens significativas em relação ao Jupyter Notebook, como
colaboração em tempo real, uma barra lateral com um gerenciador de
arquivos e a capacidade de colapsar células de código, o que melhora
bastante a legibilidade. Essas funcionalidades tornam o Jupyter Lab uma
escolha atraente para projetos mais complexos.

Neste projeto específico, vou optar por usar o Jupyter Lab,
principalmente por suas opções de interface e pela barra lateral, que me
permitem ter um melhor controle dos arquivos e uma visão mais organizada
do meu trabalho. Essas ferramentas proporcionam um ambiente de
desenvolvimento mais eficiente e agradável, especialmente ao lidar com
múltiplos arquivos e tarefas.

Para instalar o Jupyter Lab:

![](media/image16.png){width="5.905555555555556in"
height="0.7506944444444444in"}

![](media/image17.png){width="5.905555555555556in"
height="1.3479166666666667in"}

Após a instalação é só lançar:

![](media/image18.png){width="5.905555555555556in"
height="0.7055555555555556in"}

![](media/image19.png){width="5.905555555555556in"
height="1.1319444444444444in"}

**Usando o ambiente virtual dentro do Jupyter Lab**

Para usar o ambiente virtual criando nas etapas anteriores preciso
primeiramente instalar o Kernel do jupyter Lab dentro do ambiente
virtual para isso:

![](media/image20.png){width="5.905555555555556in"
height="0.7736111111111111in"}

![](media/image21.png){width="5.905555555555556in"
height="0.48194444444444445in"}

E agora instalo o ambiente dentro do jupyter lab com o comando:

![](media/image22.png){width="5.007321741032371in"
height="0.7288582677165354in"}

![](media/image23.png){width="5.9725in" height="0.4339534120734908in"}

Feito isso agora basta lançar o jupyter lab e selecionar o ambiente
virtual dentro da interface, vou em Change Kernel \> e seleciono o
Kernel recém criado.

![](media/image24.png){width="5.905555555555556in"
height="2.671527777777778in"}

![](media/image25.png){width="5.947398293963254in"
height="2.729712379702537in"}

Se eu tiver feito tudo certo, no canto direito do notebook consigo ver o
ambiente virtual em uso no projeto, no meu caso o ambiente virtual que
criei anteriormente é o venv.

![](media/image26.png){width="5.969904855643045in"
height="2.555779746281715in"}

# Conectando no Banco de Dados SQL Server

Nesta etapa vou me conectar em um banco de dados local (on premises)
usando a biblioteca pyodbc. No meu caso não vou informar o parâmetro
DATABASE pois pretendo criar o database atráves do pyodbc.

Sendo assim, vou passar somente o driver e o servidor e vou me conectar
diretamente na instância master do SQL Server. A seguir vou criar a
conexão com o banco de dados, criar um cursor e por fim fechar cursor e
conexão.

O cursor é um objeto do pyodbc que basicamente executa as queries em
sql. A referência para documentação oficial está no link a seguir:

<https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16>

Passos:

1 -- Testar a conexão

2 -- Criar um cursor

3 -- Fechar cursor e conexão

![](media/image27.png){width="5.905555555555556in" height="1.93125in"}

# Primeira Etapa do ETL: Extract

Chegamos à primeira etapa do nosso processo de ETL, que é a extração dos
dados para carregá-los em um banco de dados. Com a conexão ao SQL Server
aberta, o primeiro passo é verificar se o banco de dados necessário já
existe. Se ele não existir, vou criá-lo. Em seguida, vou verificar se as
tabelas que preciso carregar já estão presentes no banco de dados; caso
não estejam, vou criá-las.

Resumo dos passos:

1 -- Verificar se existe um database de nome ETL criado, se não existir
criá-lo;

2 -- Verificar se a tabela que quero carregar existe, se não existir
criá-la;

3 -- Repetir as etapas para todos os arquivos.

1 -- Etapa 1: Verificar se existe um database de nome ETL criado, se não
existir criá-lo.

![](media/image28.png){width="5.905555555555556in"
height="1.1479166666666667in"}

![](media/image29.png){width="5.905555555555556in"
height="3.316666666666667in"}

Após a execução do comando abaixo foi criado um banco de dados de nome
DRE na instância local do SQL Server. Isso garante que, mesmo que o
banco de dados seja deletado por engano, o ETL continue sendo executado,
pois sempre teremos a criação de um database caso ele não exsita.

2 -- Etapa 2: Verificar se a tabela que quero carregar existe, se não
existir criá-la.

Nesta etapa vou precisar instalar o sql alchemy para poder ler a tabela
e criá-la automaticamente no banco de dados caso ela não exista, para
isso utilizarei um método do pandas chamdado to_sql.

Instalando o sql alchemy e importando a função create_engine

![](media/image30.png){width="4.791666666666667in" height="1.1875in"}

![](media/image31.png){width="5.208333333333333in"
height="1.9479166666666667in"}

![](media/image32.png){width="5.905555555555556in" height="1.43125in"}

![](media/image33.png){width="5.905555555555556in"
height="1.5118055555555556in"}

Primeiro criei a string de conexão parecida com aquela criada
anteriormente no pyodbc.

Após isso criei o engine que é o objeto do sql alchemy que se conecta ao
Sql Server e após ler o arquivo utilizei o método do pandas to_sql para
inserir a tabela lida diretamente no banco de dados, o parametro
if_exists = 'replace' a cada execução substitui os dados já
preexistente.

Como podemos ver na imagem a seguir a tabela foi carregada porém trouxe
várias linhas em branco. Na etapa de extração não vou me preocupar com o
tratamento e transformações de dados e tabelas, deixarei isso para a
etapa 2 do ETL: Transform.

![](media/image34.png){width="5.872874015748032in"
height="2.4171259842519683in"}

3 -- **Etapa 3**: Repetir as etapas para todos os arquivos. Nesta etapa
vou carregar todas as tabelas que existem dentro da pasta.

Para isto vou percorrer todos os arquivos através de um loop for e
carregar os arquivos csv no banco de dados.

![](media/image35.png){width="5.905555555555556in" height="3.41875in"}

Como podemos ver na imagem a seguir todos os arquivos csv foram
carregados no banco de dados DRE.

![](media/image36.png){width="5.905555555555556in"
height="4.588888888888889in"}

Para tornar o código mais organizado e fácil de manter, vou encapsular
todas as etapas de extração de dados dentro de uma função chamada
extract(). Isso não apenas melhora a clareza e a reutilização do código,
mas também permite que algumas etapas sejam dinamicamente configuradas
por meio de parâmetros, tornando o processo mais
flexível.![](media/image37.png){width="5.833333333333333in"
height="6.520833333333333in"}

![](media/image38.png){width="5.924371172353456in"
height="2.01494750656168in"}

# Segunda Etapa do ETL: Transform

A segunda etapa do ETL é a etapa de transformação dos dados.

Nesta etapa vou verificar a tipagem dos dados, tratas valores nulos e
realizar transformações quando necessário.

Vou usar como referência as transformações aplicadas no Power Query e
replicá-las no Jupyter Lab. Por fim, encapsular todas as etapas dentro
de uma função e prosseguir com a próxima etapa: Load.

Vou começar pela tabela dEstruturaDRE. As etapas aplicadas foram as
seguintes:

![](media/image39.png){width="3.5833333333333335in"
height="4.072916666666667in"}

1 -- **Promoted Headers**: Foi usada a primeira linha como cabeçalho.
Vou pular esta etapa pois os dados já foram carregados com o cabeçalho
correto.

2 -- **Changed Type**: Mostra como foi definida a tipagem dos dados.

![](media/image40.png){width="5.905555555555556in" height="0.56875in"}

3 -- **Renamed Columns**: As colunas foram renomeadas para o Inglês.

![](media/image41.png){width="5.905555555555556in"
height="0.7784722222222222in"}

4 -- **Filtered Rows**: Aqui foram tratados os valores nulos filtrando a
coluna id diferente de nulo e vazio.

![](media/image42.png){width="5.905555555555556in"
height="0.7993055555555556in"}

Agora vou aplicar as mesmas transformações no pandas:

![](media/image43.png){width="5.905555555555556in"
height="6.623611111111111in"}

![](media/image44.png){width="4.65625in" height="3.96875in"}

**Tabela dPlanoConta**:

![](media/image45.png){width="3.6875in" height="4.125in"}

1 -- **Promoted Headers**: Como feito anteriormente, vou pular esta
etapa.

2 -- **Changed Type**: Tipagem dos dados como na imagem a seguir:

![](media/image46.png){width="5.905555555555556in"
height="0.5020833333333333in"}

3 -- **FilteredRows1**: Remoção de valores nulos.

4 -- **Renamed Columns** : Colunas renomeadas do português para o
inglês.

![](media/image47.png){width="5.905555555555556in"
height="0.5645833333333333in"}

Aplicando as mesmas transformações no pandas:

![](media/image48.png){width="5.905555555555556in"
height="5.6722222222222225in"}

![](media/image49.png){width="4.385416666666667in" height="5.375in"}

**Tabela fOrcamento**:

![](media/image50.png){width="3.5416666666666665in" height="4.5in"}

1 -- **Promoted Headers**: Etapa não necessária.

2 -- **Changed Type**: Tipagem dos dados como segue:

![](media/image51.png){width="5.905555555555556in" height="0.63125in"}

3 -- **Renamed Columns**: Renomear as colunas como segue:

![](media/image52.png){width="5.905555555555556in"
height="0.6305555555555555in"}

4 -- **Change Type with Locale** : Alterar a data de acordo com a data
local.

Aplicando as transformações no pandas:

![](media/image53.png){width="5.905555555555556in"
height="4.653472222222222in"}

![](media/image54.png){width="5.291666666666667in"
height="3.4479166666666665in"}

**Tabela fPrevisao**

![](media/image55.png){width="3.53125in" height="4.322916666666667in"}

1 -- **Promoted Headers**: Pulei esta etapa pois o pandas leu
corretamente.

2 -- **Changed Type**:Tipagem dos dados conforme abaixo:

![](media/image56.png){width="5.905555555555556in" height="0.5625in"}

3 -- **Renamed Columns**: Renomear colunas como segue:

![](media/image57.png){width="5.905555555555556in"
height="0.5638888888888889in"}

4 -- **Changed Type with Locale**: Mudar a data para data local.

Aplicando as transformações no pandas tiver um erro.

**Resolvendo Erro ao Converter Valores Numéricos de Texto para Float**

**O Problema:**

**\
**Os valores estavam com os separadores decimais como "." (ponto), mas
em Python, o ponto é o separador decimal padrão. No entanto, a presença
de múltiplos pontos em valores numéricos levou a erros na conversão.

![](media/image58.png){width="6.220234033245844in"
height="1.6092804024496938in"}

**A Solução:**\
Para resolver isso, precisei substituir o último ponto (que separa as
casas decimais) por um caractere provisório, como "#". Depois, substituí
os outros pontos por uma string vazia (""), e finalmente, troquei o "#"
de volta para ".", permitindo que o Python reconhecesse corretamente o
separador decimal.

Usei a função apply junto com uma função lambda, aplicando o método
rpartition para realizar essa substituição. O rpartition divide uma
string em três partes com base no último separador encontrado. Isso é
útil porque me permite identificar e manipular o separador decimal
corretamente.

**Exemplo do rpartition:**

Quando apliquei o rpartition, ele divide a string no último ponto, como
mostrado abaixo:

![](media/image59.png){width="5.25in" height="2.0625in"}

**Outro Desafio:**\
Notei que alguns valores inteiros no dataset não estavam no formato
xxx.00, por exemplo, o número 987. Se não tratasse esse caso, esses
números inteiros poderiam ser mal interpretados como decimais durante a
conversão, causando
erros.![](media/image60.png){width="4.364583333333333in"
height="2.03125in"}

Pois na hora de somar os números inteiros virariam valores decimais como
mostrado abaixo:

![](media/image61.png){width="5.666666666666667in"
height="4.541666666666667in"}

**A Solução para Valores Inteiros:**\
Apliquei uma regra para formatar números inteiros como xxx.00 antes de
realizar as substituições, garantindo que a conversão para float fosse
precisa.

![](media/image62.png){width="5.905555555555556in"
height="3.7020833333333334in"}

Aplicando as etapas no pandas ficaria assim:

![](media/image63.png){width="5.905555555555556in"
height="6.235416666666667in"}

Basicamente, o rpartition divide a string em uma tupla, e com o slicing
\[::2\], pego todos os elementos da tupla, ignorando o separador
decimal. Depois, uso join para concatenar o caractere provisório \# com
as outras partes da tupla. Finalmente, faço as substituições necessárias
para que o Python possa converter corretamente para float.

**Tabelas fLancamentos**

![](media/image64.png){width="3.75in" height="4.916666666666667in"}

Para as tabelas fLancamentos vou primeiramente tratar uma e como elas
têm todas as mesma estrutura, basta replicar o mesmo tratamento para as
demais através de um loop for.

Os principais tratamentos são:

1 -- **Changes Type**: Tipagem dos dados;

![](media/image65.png){width="5.905555555555556in"
height="0.5451388888888888in"}

2 -- **Renamed Columns**: Renomear as colunas

![](media/image66.png){width="5.905555555555556in"
height="0.6659722222222222in"}

3 -- **Filtered Rows1**: Remover os nulos

4 -- **Changed Type with Locale**: Alterar a data de acordo com o
formato local.

Realizando os mesmos passos no pandas:

![](media/image67.png){width="5.905555555555556in"
height="5.420833333333333in"}

![](media/image68.png){width="5.541666666666667in"
height="2.7916666666666665in"}

**Automatizando o tratamento**

Após realizar o tratamento em todas as tabelas, vou criar uma função que
encapsule todas as etapas de tratamento e transformação dos dados.

A grande vantagem dessa abordagem é a facilidade de manutenção: se novas
tabelas forem adicionadas à pasta, basta incluir o tratamento específico
para essa tabela dentro da função.

Além disso, para tornar a função mais dinâmica, permitirei que o usuário
informe qualquer caminho de pasta. Isso garante que, se os arquivos
mudarem de local, o processo de ETL não será afetado.

![](media/image69.png){width="4.520833333333333in"
height="3.3541666666666665in"}

A função completa pode ser vista no dentro do arquivo ETL_DRE.py

Basicamente a função percorre todos os arquivos em uma pasta e de acordo
com o nome realiza o tratamento necessário. Após o tratamento as tabelas
são inseridas em uma lista e retornadas pela função.

# Terceira Etapa do ETL: Load

A última etapa do ETL é o carregamento dos dados. Nesta etapa criei uma
função que carrega os dados tratados e transformados em um database
informado pelo usuário.

Os principais passos são:

1 -- Ativar a conexão do banco de dados SQL Server:

![](media/image70.png){width="4.229166666666667in"
height="3.7083333333333335in"}

2 -- Tratar os dados:

![](media/image71.png){width="5.905555555555556in"
height="3.321527777777778in"}

3 -- Criar o novo database onde os dados tratados serão armazenados:

![](media/image72.png){width="5.905555555555556in"
height="2.404166666666667in"}

4 -- Carregar os dados tratados:

![](media/image73.png){width="5.905555555555556in"
height="2.0541666666666667in"}

5 - Criando a função Load:

![](media/image74.png){width="5.905555555555556in"
height="6.038888888888889in"}

Testando a função:

![](media/image75.png){width="5.905555555555556in"
height="4.858333333333333in"}

# Criando a classe ETL

Após criar e testar individualmente as funções do ETL, é hora de
organizar tudo em uma classe. Utilizar uma classe permite estruturar o
código de forma mais limpa e eficiente, com funções definidas para cada
tarefa. No caso deste ETL específico, a classe ajuda a evitar a
redundância de código, como a necessidade de criar várias vezes a mesma
função de conexão ao banco de dados.

Ao inicializar a classe, a conexão ao banco de dados é estabelecida
automaticamente e herdada por todos os métodos. Isso não só simplifica o
código, mas também garante que a conexão seja reutilizada de maneira
consistente em todas as operações de ETL.

Outro grande benefício é que posso criar várias instâncias do ETL com
diferentes servidores e caminhos de pasta. Basta fazer cópias da classe
e ajustar os parâmetros conforme necessário, tornando o processo
altamente flexível e reutilizável.

A classe completa pode ser vista no arquivo ETL_DRE.py

# Criando o arquivo ETL.py

Após criada a classe, chegou a hora de criar o arquivo ETL.py que será
usado para automatizar todo o ETL.

A criação dele é bastante simples, basta abrir novo Python File e copiar
as bibliotecas usadas, a classe e executar os métodos do ETL,
nomeadamente: extract, transform, load.

![](media/image76.png){width="5.905555555555556in"
height="3.777083333333333in"}

![](media/image77.png){width="5.905555555555556in"
height="5.032638888888889in"}

![](media/image78.png){width="5.905555555555556in"
height="2.286111111111111in"}

Para executar o arquivo abro o terminal do PowerShell, navego até o
diretório da pasta, ativo o ambiente virtual e dou o comando abaixo:

![](media/image79.png){width="5.905555555555556in" height="3.9625in"}

Se tudo correr bem o ETL executará todas as etapas. As mensagens
printadas acima ajudam a entender em qual etapa podem ocorrer eventuais
erros.

# Criando o arquivo requirements.txt

Depois de testar o ETL e confirmar que ele está funcionando
corretamente, é importante garantir que outras pessoas possam reproduzir
o projeto em suas próprias máquinas. Para isso, vou exportar todas as
bibliotecas e dependências utilizadas no projeto para um arquivo .txt.

Para isso, no terminal do PowerShell navego até a pasta do projeto, e
digito os comandos abaixo:

![](media/image80.png){width="5.905555555555556in"
height="1.1194444444444445in"}

Isso criará um arquivo requirements.txt no diretório do projeto, que
contém a lista de todas as bibliotecas e versões específicas usadas.

Esse arquivo é geralmente incluído nos repositórios do GitHub,
permitindo que qualquer pessoa que clone o repositório possa facilmente
recriar o ambiente necessário para executar o ETL em sua máquina local.
É importante lembrar que alguns parâmetros, como o servidor do banco de
dados, precisarão ser ajustados de acordo com a configuração individual
de cada usuário.

![](media/image81.png){width="3.7105347769028874in"
height="4.51870406824147in"}[]{#_Toc175081322 .anchor}

# Automatizando o ETL com o agendador de tarefas do Windows

Para agendar a execução do ETL mediante o programador de tarefas do
Windows, seguir os seguintes passos:

1 -- Digitar na busca do Windows a palavra: "agendador de tarefas" ou
"programador de tarefas" (pt europeu).

![](media/image82.png){width="4.875in" height="9.520833333333334in"}

2 -- Após abrir o programa clicar em "Criar Tarefa
Básica":![](media/image83.png){width="5.905555555555556in"
height="4.200694444444444in"}

3 -- Nomear a tarefa e dar uma breve descrição:

![](media/image84.png){width="5.905555555555556in"
height="4.129166666666666in"}

4 -- Escolher a frequência da execução:

![](media/image85.png){width="5.905555555555556in"
height="4.145833333333333in"}

5 -- Escolher o horário e de quantos em quantos dias repetir:

![](media/image86.png){width="5.905555555555556in"
height="4.129166666666666in"}

6 -- Clicar em iniciar um programa e avançar:

![](media/image87.png){width="5.905555555555556in"
height="4.089583333333334in"}

7 -- No ponto 1 colocar o caminho de onde está instalado o arquivo
python.exe dentro do ambiente virtual usado no projeto. No item 2 será o
nome do arquivo que será executado e o item 3 é a pasta do projeto.

![](media/image88.png){width="5.905555555555556in" height="4.13125in"}

8 - Pronto! Agora basta clicar em concluir e a tarefa estará agendada.

![](media/image89.png){width="5.905555555555556in"
height="4.149305555555555in"}

9 -- Para testar a tarefa, no painel inicial basta clicar na tarefa e
clicar em executar. Se tudo der certo, um terminal será aberto e um
script executado.

![](media/image90.png){width="5.905555555555556in"
height="3.0680555555555555in"}

![](media/image91.png){width="5.905555555555556in"
height="2.6368055555555556in"}

**Observação**: O programador de tarefas do Windows funciona somente se
o computador estiver ligado. Então em um cenário onde há um servidor
local por exemplo que funciona 24 horas por dia, pode ser interessante
colocar o script para ser executado localmente. Caso contrário há outras
bibliotecas de agendamento como o scheduler que funcionam mesmo com o
computador desligado.

# Conectando Power BI no banco de dados

Tendo os dados já tratados e transformados no banco de dados, agora só
nos resta conectar no banco de dados trazendo as tabelas. Para isso:

1 -- No menu inicial do Power BI, vou em SQL Server

![](media/image92.png){width="5.905555555555556in"
height="4.042361111111111in"}

2 -- Informo o servidor e o nome do banco de dados. No meu caso o
servidor é local então coloco localhost e o nome do banco de dados que
quero acessar é
DRE_Cleaned.![](media/image93.png){width="5.905555555555556in"
height="2.9618055555555554in"}

3 -- Após isso tenho acesso as tabelas do banco de dados.

![](media/image94.png){width="5.905555555555556in" height="4.68125in"}

4 -- Agora basta selecionar e ir para transform data. Teoricamente os
dados já estão todos tratados e tipados e não precisaríamos acrescentar
etapas no Power Query, e isto tornaria o carregamento dos dados mais
rápidos.

![](media/image95.png){width="5.905555555555556in"
height="3.161111111111111in"}

# Modelagem e Relacionamentos do Modelo

No projeto, diferentes arquivos CSV estão relacionados entre si,
formando a base para o modelo de dados. Aqui está uma explicação sobre
como esses relacionamentos funcionam:

**dEstruturaDRE.csv tem um relacionamento 1:\* (um para muitos) com
dPlanoConta.csv:**

-   Isso significa que cada registro em dEstruturaDRE.csv pode estar
    relacionado a vários registros em dPlanoConta.csv. Em outras
    palavras, dEstruturaDRE.csv atua como um filtro para dPlanoConta.csv
    através do campo id.

![](media/image96.png){width="5.614583333333333in" height="3.03125in"}

**dPlanoConta.csv tem um relacionamento 1 :\* com fLancamentos.csv:**

-   Aqui, dPlanoConta.csv também filtra os registros de
    fLancamentos.csv. Um único registro em dPlanoConta.csv pode estar
    associado a muitos registros em fLancamentos.csv.

![](media/image97.png){width="5.905555555555556in"
height="5.583333333333333in"}

**dPlanoConta.csv tem um relacionamento 1:\* com fPrevisao.csv:**

-   De maneira similar, dPlanoConta.csv filtra os registros de
    fPrevisao.csv, onde um registro de dPlanoConta.csv pode corresponder
    a vários registros em fPrevisao.csv.

![](media/image98.png){width="5.905555555555556in"
height="2.890972222222222in"}

**dPlanoConta.csv tem um relacionamento 1: \*com fOrcamento.csv:**

-   Novamente, dPlanoConta.csv filtra fOrcamento.csv, com a
    possibilidade de um registro em dPlanoConta.csv estar associado a
    vários registros em fOrcamento.csv.

![](media/image99.png){width="5.905555555555556in"
height="2.977777777777778in"}

**Modelo Final:**

O modelo de dados resultante é conhecido como **snowflake** (floco de
neve), onde temos duas tabelas de dimensões normalizadas:
dPlanoConta.csv e dEstruturaDRE.csv. Diferente do modelo **StarSchema**
(esquema estrela), onde as tabelas de fatos se conectam diretamente às
dimensões sem sub-níveis de normalização, o modelo snowflake oferece uma
estrutura mais detalhada e segmentada.

![](media/image100.png){width="5.905555555555556in"
height="3.814583333333333in"}

# Análise DRE

A DRE que estou analisando é de uma cadeia de lojas em três estados: São
Paulo, Rio de Janeiro e Florianópolis. Com isso em mente, pensei em
algumas perguntas estratégicas que eu faria se fosse o dono do negócio,
e em como essas questões poderiam ser respondidas visualmente no
dashboard. As perguntas-chave são:

1.  **Como está o desempenho do grupo?**\
    (Considerando o grupo como a soma das três lojas.)

> ![](media/image101.png){width="5.905555555555556in"
> height="3.3256944444444443in"}

2.  **Como está o desempenho individual de cada loja?**

> ![](media/image102.png){width="5.905555555555556in"
> height="3.2916666666666665in"}

3.  **Mensalmente, o que foi planejado está sendo atingido ou não?**

> ![](media/image103.png){width="5.905555555555556in"
> height="3.373611111111111in"}

4.  **Consigo avaliar como o EBITDA ou outro indicador está variando ao
    longo dos meses?**

> ![](media/image104.png){width="5.905555555555556in"
> height="3.3506944444444446in"}
>
> ![](media/image105.png){width="5.905555555555556in"
> height="3.3520833333333333in"}

5.  **De tudo o que vendemos, quanto tenho de margem bruta para arcar
    com as despesas fixas?**

> ![](media/image106.png){width="5.905555555555556in"
> height="3.3513888888888888in"}

6.  Uma visão consolidade e que eu possa alterar entre análise
    horizontal e análise vertical?

![](media/image107.png){width="5.905555555555556in"
height="3.316666666666667in"}

# Conclusão e Próximos Passos

Este projeto demonstrou como a automação do processo de ETL, utilizando
Python e Power BI, pode transformar a maneira como empresas gerenciam e
analisam seus dados financeiros. Ao automatizar a extração,
transformação e carregamento dos dados para uma DRE, conseguimos criar
uma solução robusta e escalável que não só melhora a eficiência
operacional, mas também proporciona insights valiosos para a tomada de
decisões estratégicas.

O uso do Jupyter Lab e de uma estrutura de classes no Python facilitou a
organização e manutenção do código, garantindo que o processo seja
replicável e adaptável a diferentes contextos e necessidades
empresariais. Além disso, a modelagem de dados no formato snowflake
permitiu uma análise detalhada e estruturada das informações, otimizando
o desempenho das consultas no Power BI.

-   **Expansão do Dashboard:**\
    Expandir o dashboard do Power BI para incluir mais KPIs
    (indicadores-chave de desempenho) que possam ajudar a identificar
    tendências, como análise de lucratividade por produto ou segmento de
    mercado.

<!-- -->

-   **Implementação de Alertas Automatizados:**\
    Adicionar um sistema de alertas automatizados que notifique o
    usuário quando determinados indicadores, como EBITDA ou margem
    bruta, atingirem valores críticos ou fora do esperado.
