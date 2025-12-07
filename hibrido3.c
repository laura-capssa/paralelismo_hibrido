#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LINE_SIZE 4096     // Tamanho máximo de uma linha do CSV
#define INITIAL_CAP 10000  // Capacidade inicial do vetor de linhas

// -----------------------------------------------------------
// Função que processa uma linha do CSV e extrai as colunas-alvo
// Usa strtok_r (thread-safe)
// -----------------------------------------------------------
void extrair_colunas_threadsafe(const char *linha, int col_bras, int col_estr,
                                long long *out_bras, long long *out_estr) {

    char buffer[LINE_SIZE];
    // copia para buffer local (cada thread usa seu buffer)
    strncpy(buffer, linha, LINE_SIZE - 1);
    buffer[LINE_SIZE - 1] = '\0';

    const char *delim = ";";
    char *saveptr = NULL;
    char *token = strtok_r(buffer, (char *)delim, &saveptr);
    int col = 0;

    *out_bras = 0;
    *out_estr = 0;

    while (token != NULL) {
        if (col == col_bras) {
            // atoll aceita espaços/newlines; assume CSV limpo
            *out_bras = atoll(token);
        } else if (col == col_estr) {
            *out_estr = atoll(token);
        }

        token = strtok_r(NULL, (char *)delim, &saveptr);
        col++;
    }
}

int main(int argc, char *argv[]) {

    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    FILE *file;
    long file_size = 0;
    long chunk_start = 0, chunk_end = 0;

    // Configurações do CSV
    int col_bras = 4;   // índice da coluna (0-based)
    int col_estr = 5;

    // -----------------------------------------------------------
    // Rank 0 descobre o tamanho do arquivo
    // -----------------------------------------------------------
    if (rank == 0) {
        file = fopen("BRAZIL_CITIES.csv", "r");
        if (!file) {
            fprintf(stderr, "Erro ao abrir o arquivo CSV!\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Ignora o cabeçalho (se houver)
        char header[LINE_SIZE];
        if (fgets(header, LINE_SIZE, file) == NULL) {
            // arquivo vazio
            fclose(file);
            file_size = 0;
        } else {
            fseek(file, 0, SEEK_END);
            file_size = ftell(file);
            fclose(file);
        }
    }

    // Broadcast do tamanho do arquivo para todos os ranks
    MPI_Bcast(&file_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    if (file_size == 0) {
        if (rank == 0) fprintf(stderr, "Arquivo vazio ou não encontrado.\n");
        MPI_Finalize();
        return 1;
    }

    long chunk = file_size / size;
    chunk_start = rank * chunk;
    chunk_end = (rank == size - 1 ? file_size : chunk_start + chunk);

    // -----------------------------------------------------------
    // Abertura do arquivo para leitura (cada processo abre seu FILE)
    // -----------------------------------------------------------
    file = fopen("BRAZIL_CITIES.csv", "r");
    if (!file) {
        fprintf(stderr, "Rank %d: erro ao abrir o arquivo!\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Pula para o início da fatia
    if (fseek(file, chunk_start, SEEK_SET) != 0) {
        fprintf(stderr, "Rank %d: fseek falhou\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Se não começar no byte zero, descarta a primeira linha incompleta
    if (chunk_start != 0) {
        char dump[LINE_SIZE];
        fgets(dump, LINE_SIZE, file);
    }

    // -----------------------------------------------------------
    // Leitura sequencial das linhas da fatia
    // Armazenamos as linhas em memória para depois paralelizarmos o processamento
    // -----------------------------------------------------------
    char **linhas = malloc(INITIAL_CAP * sizeof(char *));
    if (!linhas) {
        fprintf(stderr, "Rank %d: malloc falhou\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    long linhas_cap = INITIAL_CAP;
    long linhas_count = 0;

    char line[LINE_SIZE];

    while (ftell(file) <= chunk_end && fgets(line, LINE_SIZE, file)) {

        // aloca mais espaço se necessário
        if (linhas_count == linhas_cap) {
            linhas_cap *= 2;
            char **tmp = realloc(linhas, linhas_cap * sizeof(char *));
            if (!tmp) {
                fprintf(stderr, "Rank %d: realloc falhou\n", rank);
                // libera o que já alocou
                for (long i = 0; i < linhas_count; i++) free(linhas[i]);
                free(linhas);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            linhas = tmp;
        }

        // guarda a linha (inclui newline se houver)
        linhas[linhas_count] = malloc(strlen(line) + 1);
        if (!linhas[linhas_count]) {
            fprintf(stderr, "Rank %d: malloc linha falhou\n", rank);
            for (long i = 0; i < linhas_count; i++) free(linhas[i]);
            free(linhas);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        strcpy(linhas[linhas_count], line);
        linhas_count++;
    }

    fclose(file);

    // -----------------------------------------------------------
    // PROCESSAMENTO EM PARALELO COM OPENMP (thread-safe)
    // Cada thread usa uma cópia local do buffer (extrair_colunas_threadsafe)
    // -----------------------------------------------------------
    long long local_bras = 0;
    long long local_estr = 0;

    // Se linhas_count for 0, pulamos o for
    #pragma omp parallel for reduction(+:local_bras, local_estr) schedule(static)
    for (long i = 0; i < linhas_count; i++) {

        char *linha = linhas[i];

        // Ignora possíveis cabeçalhos espalhados
        if (strstr(linha, "IBGE_RES_POP_BRAS") != NULL)
            continue;

        long long pop_bras = 0;
        long long pop_estr = 0;

        extrair_colunas_threadsafe(linha, col_bras, col_estr, &pop_bras, &pop_estr);

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
    // Impressão final (rank 0)
    // -----------------------------------------------------------
    if (rank == 0) {
        double proporcao = total_bras > 0 ? (double) total_estr / total_bras * 100.0 : 0.0;

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
