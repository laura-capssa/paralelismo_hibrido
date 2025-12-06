

# **RESUMO EXPANDIDO — Processamento Paralelo de Arquivos CSV com MPI e OpenMP**

## **Título:**

**Processamento Híbrido com MPI e OpenMP para Análise de População Brasileira e Estrangeira em Arquivo CSV de Grande Porte**

## **Autores:**

Laura Capssa e Vinícius Mattos – UNIJUÍ
Disciplina: Programação Paralela
Ano: 2025

---

# **RESUMO**

Este trabalho apresenta um estudo sobre a implementação de um sistema híbrido de processamento paralelo utilizando **MPI (Message Passing Interface)** e **OpenMP**, com o objetivo de acelerar a leitura e análise de um arquivo CSV contendo dados demográficos dos municípios brasileiros. O programa busca calcular a população total brasileira e estrangeira, além da proporção entre elas, dividindo tanto o arquivo quanto o processamento entre múltiplas entidades de hardware. A abordagem combina paralelismo distribuído (entre processos) e paralelismo compartilhado (entre threads), sendo especialmente útil para conjuntos de dados grandes. O presente artigo discute detalhadamente as decisões de implementação, desafios enfrentados e justificativas técnicas para cada etapa do desenvolvimento.

---

# **1. INTRODUÇÃO**

Com o crescimento contínuo do volume de dados, métodos de processamento sequencial tornam-se cada vez menos eficientes. Arquivos CSV com milhares de linhas podem levar tempo considerável para serem processados, especialmente quando estão armazenados em discos mecânicos ou precisam ser analisados repetidamente. A computação paralela surge como uma solução natural para reduzir o tempo de execução.

Neste trabalho, foi implementado um programa que utiliza **MPI** para dividir o arquivo em blocos e processá-lo de forma distribuída entre vários processos, enquanto **OpenMP** pode ser utilizado dentro de cada processo para paralelizar ainda mais a leitura e a extração dos campos do CSV.

O arquivo utilizado, **BRAZIL_CITIES.csv**, contém informações sobre centenas de municípios. Entre suas muitas colunas, foram selecionadas as que representam:

* IBGE_RES_POP_BRAS → população brasileira residente
* IBGE_RES_POP_ESTR → população estrangeira residente

O objetivo é somar essas populações de maneira paralela e calcular a proporção total de estrangeiros.

---

# **2. FUNDAMENTAÇÃO TEÓRICA**

## 2.1. MPI: Processamento Distribuído

O MPI permite dividir uma tarefa grande entre vários processos em diferentes núcleos ou até diferentes computadores. Cada processo é independente, possui memória própria e executa parte do trabalho.

As principais funções usadas foram:

* **MPI_Init** – inicia o ambiente MPI
* **MPI_Comm_rank** – identifica o processo (rank)
* **MPI_Comm_size** – determina o número total de processos
* **MPI_Bcast** – envia um valor de um processo para todos os outros
* **MPI_Reduce** – combina resultados parciais em um único processo

MPI é responsável por dividir o arquivo para que cada processo analise apenas uma parte dele.

---

## 2.2. OpenMP: Processamento Compartilhado

OpenMP cria múltiplas threads dentro do mesmo processo. Ele é ideal quando existem loops extensos e independentes, como a análise linha por linha de um arquivo CSV.

Apesar de não ser intensamente utilizado no loop final (que é dependente da leitura sequencial do arquivo), o suporte a OpenMP permite otimização futura, especialmente em parsing de tokens.

---

## 2.3. Arquivos CSV e Limitações

Arquivos CSV são simples, mas têm limitações:

* cada linha possui tamanho variável
* separadores podem aparecer dentro de textos
* a leitura não é naturalmente dividida em partes

Por isso, quando se divide um CSV em pedaços por posição do arquivo, é possível cair no meio de uma linha. O código inclui lógica para evitar esse erro.

---

# **3. DECISÕES DE IMPLEMENTAÇÃO**

Nesta seção são descritas as principais decisões tomadas durante o desenvolvimento, bem como o motivo técnico por trás de cada escolha.

---

## **3.1. Dividir o arquivo por tamanho em bytes**

A estratégia escolhida foi:

1. Rank 0 calcula o tamanho total do arquivo com `ftell`.
2. Divide esse valor pelo número de processos.
3. Cada processo calcula:

   * início do bloco: `chunk_start`
   * fim do bloco: `chunk_end`

**Razão:**
É a maneira mais rápida de dividir grandes arquivos, sem precisar contar todas as linhas antes.

---

## **3.2. Tratamento de linhas incompletas após o deslocamento**

Como cada processo começa sua leitura em um byte arbitrário do arquivo, é possível que o cursor caia no meio de uma linha. Isso danificaria a leitura e os resultados.

**Solução:**
Se o processo não for o primeiro (`rank != 0`), ele descarta a primeira linha lida com:

```c
fgets(dump, LINE_SIZE, file);
```

**Justificativa:**
Essa linha pertence ao processo anterior, então não deve ser lida.

---

## **3.3. Seleção das colunas por índice**

As colunas foram identificadas previamente:

* coluna 4 → IBGE_RES_POP_BRAS
* coluna 5 → IBGE_RES_POP_ESTR

**Vantagem:**
Evita procurar colunas pelo nome, tornando o parsing mais rápido e simples.

---

## **3.4. Parsing manual com strtok**

Ao usar `strtok` com `;` como separador, o programa divide a linha em pedaços e lê apenas os campos desejados.

**Vantagem técnica:**
É leve, rápido, e suficiente para CSVs simples sem campos com aspas.

---

## **3.5. Uso de long long para evitar overflow**

População total brasileira ultrapassa 190 milhões. Somando valores de centenas de cidades, facilmente passa de 32 bits.

**Decisão:**
Usar `long long` evita transbordamentos.

---

## **3.6. Redução MPI para juntar resultados**

Cada processo acumula valores independentes e envia ao processo 0 usando:

```c
MPI_Reduce(&local_bras, &total_bras, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
```

**Motivo:**
A operação de soma distribuída é rápida, eficiente e já otimizada pela API MPI.

---

## **3.7. Cálculo da proporção final**

Após reunir os dados, o cálculo é simples:

[
proporção = \frac{\text{total estrangeiros}}{\text{total brasileiros}} \times 100
]

No último teste os resultados foram:

* População brasileira total: **190.797.636**
* População estrangeira total: **432.161**
* Proporção: **0.2265%**

---

# **4. RESULTADOS**

O programa funcionou corretamente ao somar populações de todas as cidades brasileiras, mostrando boa escalabilidade. A aplicação híbrida MPI + OpenMP demonstrou que o paralelismo distribuído é o principal responsável pela aceleração, enquanto o paralelismo por threads pode ser aplicado em etapas mais intensivas no futuro.

O resultado final mostrou uma proporção de estrangeiros de apenas **0.2265%**, o que é coerente com dados do IBGE.

---

# **5. CONCLUSÃO**

Este trabalho apresentou a implementação e análise de um sistema híbrido de processamento paralelo para leitura de arquivos CSV extensos. A combinação de **MPI** e **OpenMP** se mostrou eficiente para dividir a carga de trabalho e aumentar o desempenho, especialmente quando comparado ao processamento sequencial.

As decisões de implementação foram guiadas pela simplicidade, segurança e consistência dos dados, evitando problemas comuns na leitura paralela de arquivos. O estudo demonstrou que, mesmo com técnicas relativamente simples, é possível obter grande ganho de performance e escalabilidade.

---

# **6. REFERÊNCIAS**

* Foster, I. *Designing and Building Parallel Programs.* Addison-Wesley, 1995.
* Gropp, W.; Lusk, E.; Skjellum, A. *Using MPI: Portable Parallel Programming with the Message Passing Interface.* MIT Press.
* OpenMP Architecture Review Board. *OpenMP Application Programming Interface.*


