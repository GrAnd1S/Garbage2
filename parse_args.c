#include <unistd.h>
#include<getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "parse_args.h"

Filters parse_args(int argc, char *argv[]) {
    Filters filters = {{"", "", "", "", "", "", "", ""}, 0, 0, 0, 0, NULL, NULL, 0};
    int opt;

    while ((opt = getopt(argc, argv, "t:c:p:s")) != -1) {
        switch (opt) {
            case 't':
                for (size_t i = 0; optarg[i] != '\0' && filters.waste_type_count < 8; ++i) {
                    filters.waste_types[filters.waste_type_count][0] = optarg[i];
                    filters.waste_types[filters.waste_type_count][1] = '\0';
                    filters.waste_type_count++;
                }
                break;
            case 'c':
                sscanf(optarg, "%d-%d", &filters.capacity_min, &filters.capacity_max);
                break;
            case 'p':
                // Modify support for public filter
                if (optarg[0] == 'N') {
                    filters.public_filter = 1;
                } else if (optarg[0] == 'Y') {
                    filters.public_filter = -1;
                } else {
                    fprintf(stderr, "Invalid value for public_filter. Use 'Y' or 'N'.\n");
                    exit(EXIT_FAILURE);
                }
                break;
            case 's':
                filters.special_flag = 1;
                break;
            default:
                fprintf(stderr,
                        "Usage: %s [-t waste_type] [-c min_capacity-max_capacity] [-p public_filter] containers_file paths_file\n",
                        argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (optind + 1 >= argc) {
        fprintf(stderr, "Expected containers_file and paths_file arguments\n");
        exit(EXIT_FAILURE);
    }

    filters.containers_path = argv[optind];
    filters.paths_path = argv[optind + 1];

    return filters;
}
