#include "libs/cut.h"
#include "libs/utils.h"
#include "libs/mainwrap.h"

#include <stdlib.h>

/* Macros for file paths */
#define CONTAINERS_FILE "../tests/data/example-containers.csv"
#define PATHS_FILE "../tests/data/example-paths.csv"
#define NONEXISTENT_FILE "nonexistent.csv"

/* Tests from assignment */
/* #desc: Výpis dat */
TEST(nanecisto_vypis)
{
    CHECK(app_main_args(CONTAINERS_FILE, PATHS_FILE) == 0);

    const char *correct_output =
        "ID: 1, Type: Colored glass, Capacity: 1550, Address: Drozdi 55, Neighbors: 4\n"
        "ID: 2, Type: Clear glass, Capacity: 1550, Address: Drozdi 55, Neighbors: 4\n"
        "ID: 3, Type: Plastics and Aluminium, Capacity: 1100, Address: Drozdi 55, Neighbors: 4\n"
        "ID: 4, Type: Colored glass, Capacity: 900, Address: Drozdi 55, Neighbors: 1 2 3 5 8\n"
        "ID: 5, Type: Paper, Capacity: 5000, Address: Klimesova 60, Neighbors: 4 8\n"
        "ID: 6, Type: Colored glass, Capacity: 3000, Address: Klimesova 60, Neighbors: 8\n"
        "ID: 7, Type: Plastics and Aluminium, Capacity: 5000, Address: Klimesova 60, Neighbors: 8\n"
        "ID: 8, Type: Biodegradable waste, Capacity: 3000, Address: Na Buble 5, Neighbors: 4 5 6 7 11\n"
        "ID: 9, Type: Textile, Capacity: 500, Address: Na Buble 5, Neighbors: 10\n"
        "ID: 10, Type: Plastics and Aluminium, Capacity: 900, Address: Odlehla 70, Neighbors: 9\n"
        "ID: 11, Type: Paper, Capacity: 2000, Address: Odlehla 70, Neighbors: 8\n"
    ;
    
    ASSERT_FILE(stdout, correct_output);
    CHECK_IS_EMPTY(stderr);    
}

/* #desc: Filtrování dat */
TEST(nanecisto_filtr)
{
    CHECK(app_main_args("-t", "PA", "-c", "1000-2000", CONTAINERS_FILE, PATHS_FILE) == 0);

    const char *correct_output =
        "ID: 3, Type: Plastics and Aluminium, Capacity: 1100, Address: Drozdi 55, Neighbors: 4\n"
        "ID: 11, Type: Paper, Capacity: 2000, Address: Odlehla 70, Neighbors: 8\n"
    ;

    ASSERT_FILE(stdout, correct_output);
    CHECK_IS_EMPTY(stderr);
}

/* #desc: Výpis stanovišť */
TEST(nanecisto_sites)
{
    CHECK(app_main_args("-s", CONTAINERS_FILE, PATHS_FILE) == 0);

    const char *correct_output =
        "1;AGC;2\n"
        "2;C;1,3,4\n"
        "3;APC;2,4\n"
        "4;BT;2,3,5\n"
        "5;AP;4\n"
    ;

    ASSERT_FILE(stdout, correct_output);
    CHECK_IS_EMPTY(stderr);
}

/* Testy ošetření chyb */
/* #desc: Neexistující vstupní soubor */
TEST(nanecisto_nonexistent_input_file)
{
    CHECK(app_main_args(NONEXISTENT_FILE, PATHS_FILE) != 0);

    CHECK_NOT_EMPTY(stderr);
}

/* #desc: Chybný přepínač */
TEST(nanecisto_wrong_option)
{
    CHECK(app_main_args("-u", CONTAINERS_FILE, PATHS_FILE) != 0);

    CHECK_NOT_EMPTY(stderr);
}
