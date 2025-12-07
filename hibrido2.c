#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LINE_SIZE 4096     // Tamanho máximo de uma linha do CSV
#define INITIAL_CAP 10000  // Capacidade inicial do vetor de linhas

// -----------------------------------------------------------
// Função que processa uma linha do CSV e extrai as colunas-alvo
// -----------------------------------------------------------
void extrair_colunas(char *linha, char sep, int col_bras, int col_estr,
                     long *out_bras, long *out_estr) {

    char buffer[LINE_SIZE];
    strcpy(buffer, linha);

    char *token = strtok(buffer, &sep);
    int col = 0;

    *out_bras = 0;
    *out_estr = 0;

    while (token != NULL) {

        if (col == col_bras) {
            *out_bras = atoll(token);
        }
        if (col == col_estr) {
            *out_estr = atoll(token);
        }

        token = strtok(NULL, &sep);
        col++;
    }
}

int main(int argc, char *argv[]) {

    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    FILE *file;
    long file_size;
    long chunk_start, chunk_end;

    // Configurações do CSV
    char sep = ';';
    int col_bras = 4;
    int col_estr = 5;

    // -----------------------------------------------------------
    // Rank 0 descobre o tamanho do arquivo
    // -----------------------------------------------------------
    if (rank == 0) {
        file = fopen("BRAZIL_CITIES.csv", "r");
        if (!file) {
            printf("Erro ao abrir o arquivo CSV!\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Ignora o cabeçalho
        char header[LINE_SIZE];
        fgets(header, LINE_SIZE, file);

        fseek(file, 0, SEEK_END);
        file_size = ftell(file);

        fclose(file);
    }

    // Broadcast do tamanho do arquivo
    MPI_Bcast(&file_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    long chunk = file_size / size;

    chunk_start = rank * chunk;
    chunk_end   = (rank == size - 1 ? file_size : chunk_start + chunk);

    // -----------------------------------------------------------
    // Abertura do arquivo para leitura
    // -----------------------------------------------------------
    file = fopen("BRAZIL_CITIES.csv", "r");
    if (!file) {
        printf("Rank %d: erro ao abrir o arquivo!\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Pula para o início da fatia
    fseek(file, chunk_start, SEEK_SET);

    // Se não for o início do arquivo, pule linha incompleta
    if (chunk_start != 0) {
        char dump[LINE_SIZE];
        fgets(dump, LINE_SIZE, file);
    }

    // -----------------------------------------------------------
    // Leitura sequencial das linhas da fatia
    // (armazenamos as linhas em memória)
    // -----------------------------------------------------------
    char **linhas = malloc(INITIAL_CAP * sizeof(char *));
    long linhas_cap = INITIAL_CAP;
    long linhas_count = 0;

    char line[LINE_SIZE];

    while (ftell(file) <= chunk_end && fgets(line, LINE_SIZE, file)) {

        if (linhas_count == linhas_cap) {
            linhas_cap *= 2;
            linhas = realloc(linhas, linhas_cap * sizeof(char *));
        }

        linhas[linhas_count] = malloc(strlen(line) + 1);
        strcpy(linhas[linhas_count], line);
        linhas_count++;
    }

    fclose(file);

    // -----------------------------------------------------------
    // PROCESSAMENTO EM PARALELO COM OPENMP
    // -----------------------------------------------------------
    long long local_bras = 0;
    long long local_estr = 0;

    #pragma omp parallel for reduction(+:local_bras, local_estr) schedule(static)
    for (long i = 0; i < linhas_count; i++) {

        char *linha = linhas[i];

        // Se por acaso encontrar cabeçalho no meio:
        if (strstr(linha, "IBGE_RES_POP_BRAS"))
            continue;

        long pop_bras, pop_estr;

        extrair_colunas(linha, sep, col_bras, col_estr, &pop_bras, &pop_estr);

        local_bras += pop_bras;
        local_estr += pop_estr;
    }

    // -----------------------------------------------------------
    // REDUÇÃO GLOBAL MPI
    // -----------------------------------------------------------
    long long total_bras = 0, total_estr = 0;

    MPI_Reduce(&local_bras, &total_bras, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_estr, &total_estr, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // -----------------------------------------------------------
    // Impressão final
    // -----------------------------------------------------------
    if (rank == 0) {
        double proporcao = total_bras > 0 ? 
                           (double) total_estr / total_bras * 100.0 : 0;

        printf("\n=========== RESULTADOS FINAIS (MPI + OpenMP) ===========\n");
        printf("População brasileira total:   %lld\n", total_bras);
        printf("População estrangeira total:  %lld\n", total_estr);
        printf("Proporção geral de estrangeiros: %.4f%%\n", proporcao);
        printf("========================================================\n\n");
    }

    // Libera linhas
    for (long i = 0; i < linhas_count; i++)
        free(linhas[i]);
    free(linhas);

    MPI_Finalize();
    return 0;
}
