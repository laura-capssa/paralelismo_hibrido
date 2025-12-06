#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LINE_SIZE 4096  // Tamanho máximo de uma linha do CSV

int main(int argc, char *argv[]) {

    // Inicializa o ambiente MPI (obrigatório)
    MPI_Init(&argc, &argv__);

    int rank, size;

    // Obtém o ID do processo atual (rank)
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Obtém quantos processos MPI estão rodando
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    FILE *file;
    long file_size;

    long chunk_start, chunk_end;

    // ==== CONFIGURAÇÕES DO CSV ====
    char sep = ';';      // Separador usado no CSV
    int col_bras = 4;    // Coluna com população brasileira
    int col_estr = 5;    // Coluna com população estrangeira

    // ================================
    // Apenas o RANK 0 vai abrir o arquivo para descobrir o tamanho dele
    // ================================
    if (rank == 0) {

        file = fopen("BRAZIL_CITIES.csv", "r");
        if (!file) {
            printf("Erro ao abrir o arquivo CSV!\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Lê e ignora o cabeçalho
        char header[LINE_SIZE];
        fgets(header, LINE_SIZE, file);

        // Vai até o final do arquivo e pega o tamanho em bytes
        fseek(file, 0, SEEK_END);
        file_size = ftell(file);

        fclose(file);
    }

    // Envia o tamanho do arquivo para TODOS os processos MPI
    MPI_Bcast(&file_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    // Cada processo pegará uma "fatia" do arquivo
    long chunk = file_size / size;

    // Início da parte do arquivo que esse processo deve ler
    chunk_start = rank * chunk;

    // Fim da parte (último pega o resto do arquivo)
    chunk_end = (rank == size - 1 ? file_size : chunk_start + chunk);

    // Abre novamente o arquivo para leitura
    file = fopen("BRAZIL_CITIES.csv", "r");
    if (!file) {
        printf("Rank %d: erro ao abrir o arquivo!\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Pula direto para o início da parte que este processo deve ler
    fseek(file, chunk_start, SEEK_SET);

    // Se não for o processo 0, avança até terminar a linha,
    // assim evitamos começar no MEIO de uma linha quebrada.
    if (chunk_start != 0) {
        char dump[LINE_SIZE];
        fgets(dump, LINE_SIZE, file);
    }

    char line[LINE_SIZE];

    // Contadores locais (somados depois com MPI_Reduce)
    long long local_bras = 0;
    long long local_estr = 0;

    // ================================
    // LOOP PRINCIPAL – cada processo lê sua parte do arquivo
    // ================================
    while (ftell(file) <= chunk_end && fgets(line, LINE_SIZE, file)) {

        // Ignora cabeçalho se aparecer no meio do arquivo
        if (strstr(line, "IBGE_RES_POP_BRAS"))
            continue;

        long pop_bras = 0, pop_estr = 0;

        // Copia a linha porque o strtok altera a string
        char buffer[LINE_SIZE];
        strcpy(buffer, line);

        int col = 0;
        char *token;

        // Quebra a linha usando o separador ";"
        token = strtok(buffer, &sep);

        // Percorre cada coluna da linha
        while (token) {

            // Coluna da população brasileira
            if (col == col_bras)
                pop_bras = atoll(token);

            // Coluna da população estrangeira
            if (col == col_estr)
                pop_estr = atoll(token);

            token = strtok(NULL, &sep);
            col++;
        }

        // Soma os valores encontrados
        local_bras += pop_bras;
        local_estr += pop_estr;
    }

    fclose(file);

    // ================================
    // REDUÇÃO GLOBAL
    // Soma todos os locais em um único valor (no rank 0)
    // ================================
    long long total_bras = 0, total_estr = 0;

    MPI_Reduce(&local_bras, &total_bras, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_estr, &total_estr, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // ================================
    // APENAS O PROCESSO 0 IMPRIME RESULTADOS
    // ================================
    if (rank == 0) {
        double proporcao = total_bras > 0 ? (double)total_estr / total_bras * 100.0 : 0;

        printf("\n=========== RESULTADOS FINAIS ===========\n");
        printf("População brasileira total: %lld\n", total_bras);
        printf("População estrangeira total: %lld\n", total_estr);
        printf("Proporção geral de estrangeiros: %.4f%%\n", proporcao);
        printf("=========================================\n\n");
    }

    // Finaliza ambiente MPI
    MPI_Finalize();
    return 0;
}
