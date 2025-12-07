#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define LINE_SIZE 4096

// remove espaços iniciais e finais (in-place)
static void trim_inplace(char *s) {
    char *p = s;
    // trim leading
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) memmove(s, p, strlen(p) + 1);

    // trim trailing
    size_t len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1])) {
        s[len - 1] = '\0';
        len--;
    }
}

int main(int argc, char *argv[]) {
    const char *filename = "BRAZIL_CITIES.csv";
    if (argc >= 2) filename = argv[1];

    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Erro ao abrir o arquivo: %s\n", filename);
        return 1;
    }

    char line[LINE_SIZE];
    const char *delim = ";";
    const int col_bras = 4; // coluna IBGE_RES_POP_BRAS (0-based)
    const int col_estr = 5; // coluna IBGE_RES_POP_ESTR (0-based)

    long long total_bras = 0;
    long long total_estr = 0;
    long long linhas = 0;

    // tenta pular cabeçalho (se existir)
    if (fgets(line, LINE_SIZE, file) == NULL) {
        fprintf(stderr, "Arquivo vazio: %s\n", filename);
        fclose(file);
        return 1;
    }

    // Se a primeira linha contiver o nome da coluna, é cabeçalho
    if (strstr(line, "IBGE_RES_POP_BRAS") == NULL) {
        // não é cabeçalho → processa desde o início
        rewind(file);
    }

    while (fgets(line, LINE_SIZE, file)) {
        linhas++;

        char buffer[LINE_SIZE];
        strncpy(buffer, line, LINE_SIZE - 1);
        buffer[LINE_SIZE - 1] = '\0';

        char *saveptr = NULL;
        char *token = strtok_r(buffer, (char *)delim, &saveptr);
        int col = 0;
        long long pop_bras = 0;
        long long pop_estr = 0;
        int found_bras = 0, found_estr = 0;

        while (token != NULL) {
            trim_inplace(token);

            if (col == col_bras) {
                pop_bras = (token[0] != '\0') ? atoll(token) : 0;
                found_bras = 1;
            } 
            else if (col == col_estr) {
                pop_estr = (token[0] != '\0') ? atoll(token) : 0;
                found_estr = 1;
            }

            if (found_bras && found_estr)
                break;

            token = strtok_r(NULL, (char *)delim, &saveptr);
            col++;
        }

        total_bras += pop_bras;
        total_estr += pop_estr;
    }

    fclose(file);

    double proporcao = 0.0;
    if (total_bras > 0)
        proporcao = (double)total_estr / (double)total_bras * 100.0;

    printf("\n=========== RESULTADOS FINAIS ===========\n");
    printf("Arquivo lido: %s\n", filename);
    printf("Linhas processadas: %lld\n", linhas);
    printf("População brasileira total:   %lld\n", total_bras);
    printf("População estrangeira total:  %lld\n", total_estr);
    printf("Proporção geral de estrangeiros: %.4f%%\n", proporcao);
    printf("========================================================\n\n");

    return 0;
}
