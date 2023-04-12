#include "data_source.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <math.h>
//#include "container.h"

// Container CSV column header
#define CONTAINER_COLUMNS_COUNT 9

#define CONTAINER_ID 0
#define CONTAINER_X 1
#define CONTAINER_Y 2
#define CONTAINER_WASTE_TYPE 3
#define CONTAINER_CAPACITY 4
#define CONTAINER_NAME 5
#define CONTAINER_STREET 6
#define CONTAINER_NUMBER 7
#define CONTAINER_PUBLIC 8

// Path CSV column header
#define PATH_COLUMNS_COUNT 3

#define PATH_A 0
#define PATH_B 1
#define PATH_DISTANCE 2

struct data_source {
    char ***containers;
    size_t containers_count;

    char ***paths;
    size_t paths_count;
};

typedef struct {
    const char *id;
    double distance;
} Neighbor;

typedef struct {
    size_t id;
    const char* cid;
    double x;   // Latitude
    double y;   // Longitude
    char *waste_types;
    size_t *containerIds;
    size_t containers_count;
    size_t *neighbors;
    size_t neighbors_count;
} Station;


static struct data_source *data_source;

static char *readline(FILE *file) {
    assert(file != NULL);

    size_t line_size = 32;
    size_t offset = 0;
    char *line = NULL;
    size_t add = 32;

    char *tmp;
    
    do {
        tmp = realloc(line, line_size + 1);
        if (tmp == NULL) {
            free(line);
            return NULL;
        }

        line = tmp;
        memset(line + (line_size - add), 0, add);

        if (fgets(line + offset, add + 1, file) == NULL) {
            break;
        }

        offset = line_size;
        line_size += add;

    } while (strchr(line, '\n') == NULL);

    if (ferror(file) || feof(file)) {
        free(line);
        return NULL;
    }

    line[strlen(line) - 1] = '\0';

    return line;
}

static void free_pointer_array(void **array, size_t size) {
    assert(array != NULL);

    for (size_t index = 0; index < size; index++) {
        free(array[index]);
    }

    free(array);
}

static void free_splitted_lines(char ***lines, int column_count) {
    assert(lines != NULL);

    for (int index = 0; lines[index] != NULL; index++) {
        free_pointer_array((void **) lines[index], column_count);
    }

    free(lines);
}

static char **split_csv_line(char *line, size_t expected_count) {
    assert(line != NULL);

    char **splitted_line = calloc(expected_count + 1, sizeof(char *));
    if (splitted_line == NULL) {
        return NULL;
    }
    size_t line_length = strlen(line);

    char *token = strtok(line, ",");
    size_t parsed_length = 0;

    for (size_t index = 0; index < expected_count; index++) {
        if (token == NULL) {
            free_pointer_array((void **) splitted_line, index);
            return NULL;
        }

        parsed_length += strlen(token) + 1;

        splitted_line[index] = malloc(strlen(token) + 1);
        if (splitted_line[index] == NULL) {
            free_pointer_array((void **) splitted_line, index);
            return NULL;
        }

        strcpy(splitted_line[index], token);

        if (line_length > parsed_length
            && strchr(line + parsed_length, ',') == line + parsed_length) {
            token = "";
        } else {
            token = strtok(NULL, ",");
        }
    }

    if (token != NULL) {
        free_pointer_array((void **) splitted_line, expected_count);
        return NULL;
    }

    return splitted_line;
}

static char ***parse_csv(const char *path, int column_count) {
    assert(path != NULL);
    
    FILE *csv_file = fopen(path, "r");

    if (csv_file == NULL) {

        return NULL;
    }

    size_t array_capacity = 8;
    size_t line_count = 0;
    char ***lines = calloc(array_capacity + 1, sizeof(char **));

    if (lines == NULL) {
        fclose(csv_file);
        return NULL;
    }

    char *line;
    void *tmp;
    
    while ((line = readline(csv_file)) != NULL) {
        if (line_count >= array_capacity) {
            tmp = realloc(lines, (array_capacity * 2 + 1) * sizeof(char **));
            if (tmp == NULL) {
                free(line);
                free_splitted_lines(lines, column_count);
                fclose(csv_file);
                return NULL;
            }
            lines = tmp;
            memset(lines + line_count, 0, (array_capacity + 1) * sizeof(char **));
            array_capacity *= 2;
        }

        if ((lines[line_count] = split_csv_line(line, column_count)) == NULL) {
            free(line);
            free_splitted_lines(lines, column_count);
            fclose(csv_file);
            return NULL;
        }

        line_count++;
        free(line);
    }

    if (ferror(csv_file)) {
        free_splitted_lines(lines, column_count);
        fclose(csv_file);
        return NULL;
    }

    fclose(csv_file);
    return lines;
}

static size_t count_lines(void **lines) {
    size_t lines_count = 0;
    while (lines[lines_count] != NULL) {
        lines_count++;
    }
    return lines_count;
}

bool init_data_source(const char *containers_path, const char *paths_path) {
    data_source = malloc(sizeof(struct data_source));
        
    if (data_source == NULL) {
        return false;
    }

    data_source->containers = parse_csv(containers_path, CONTAINER_COLUMNS_COUNT);
    
    if (data_source->containers == NULL) {
        free(data_source);
        fprintf(stderr, "Invalid File %s.\n", containers_path);
        return false;
    }
    data_source->containers_count = count_lines((void **) data_source->containers);

    data_source->paths = parse_csv(paths_path, PATH_COLUMNS_COUNT);
    if (data_source->paths == NULL) {
        free_splitted_lines(data_source->containers, CONTAINER_COLUMNS_COUNT);
        free(data_source);
        return false;
    }
    data_source->paths_count = count_lines((void **) data_source->paths);

    return true;
}

void destroy_data_source(void) {
    free_splitted_lines(data_source->containers, CONTAINER_COLUMNS_COUNT);
    free_splitted_lines(data_source->paths, PATH_COLUMNS_COUNT);
    free(data_source);
}

const char *get_container_id(size_t line_index) {
    if (line_index >= data_source->containers_count) {
        return NULL;
    }
    return data_source->containers[line_index][CONTAINER_ID];
}

const char *get_container_x(size_t line_index) {
    if (line_index >= data_source->containers_count) {
        return NULL;
    }
    return data_source->containers[line_index][CONTAINER_X];
}

const char *get_container_y(size_t line_index) {
    if (line_index >= data_source->containers_count) {
        return NULL;
    }
    return data_source->containers[line_index][CONTAINER_Y];
}

const char *get_container_waste_type(size_t line_index) {
    if (line_index >= data_source->containers_count) {
        return NULL;
    }
    return data_source->containers[line_index][CONTAINER_WASTE_TYPE];
}

const char *get_path_a_id(size_t line_index) {
    if (line_index >= data_source->paths_count) {
        return NULL;
    }
    return data_source->paths[line_index][PATH_A];
}

const char *get_path_b_id(size_t line_index) {
    if (line_index >= data_source->paths_count) {
        return NULL;
    }
    return data_source->paths[line_index][PATH_B];
}

const char *get_path_distance(size_t line_index) {
    if (line_index >= data_source->paths_count) {
        return NULL;
    }
    return data_source->paths[line_index][PATH_DISTANCE];
}

Neighbor *find_neighbors(const char *given_container_id, size_t *neighbors_count) {
    Neighbor *neighbors = NULL;
    *neighbors_count = 0;

    for (size_t i = 0; i < data_source->paths_count; i++) {
        const char *container_a_id = get_path_a_id(i);
        const char *container_b_id = get_path_b_id(i);

        if (strcmp(given_container_id, container_a_id) == 0 ||
            strcmp(given_container_id, container_b_id) == 0) {
            const char *neighbor_id = strcmp(given_container_id, container_a_id) == 0 ? container_b_id : container_a_id;
            double distance = strtod(get_path_distance(i), NULL);

            neighbors = realloc(neighbors, (*neighbors_count + 1) * sizeof(Neighbor));
            neighbors[*neighbors_count].id = neighbor_id;
            neighbors[*neighbors_count].distance = distance;
            (*neighbors_count)++;
        }
    }

    Neighbor temp;
  //Sort array using the Babble Sort algorithm
  for(size_t i=0; i<*neighbors_count; i++){
    for(size_t j=0; j<*neighbors_count-1-i; j++){
      if(atoi(neighbors[j].id)> atoi(neighbors[j+1].id)){
        //swap array[j] and array[j+1]
        temp = neighbors[j];
        neighbors[j]=neighbors[j+1];
        neighbors[j+1]=temp;
      }
    }
 }   

    int unique_count = 0;
    for(size_t i = 0; i < *neighbors_count-1; i++){
        if(atoi(neighbors[i].id)!= atoi(neighbors[i+1].id)){
            neighbors[unique_count++] = neighbors[i];
        }
    }

    if(*neighbors_count > 0){
        neighbors[unique_count++] = neighbors[*neighbors_count - 1];
    }

    *neighbors_count = unique_count;
    
    return neighbors; // Caller should free the memory allocated for neighbors
}

void print_containers(Filters filters) {
    for (size_t i = 0; i < data_source->containers_count; i++) {
        const char *type = data_source->containers[i][CONTAINER_WASTE_TYPE];
        int capacity = atoi(data_source->containers[i][CONTAINER_CAPACITY]);
        int public_value = atoi(data_source->containers[i][CONTAINER_PUBLIC]);

        bool waste_type_match = false;
        if (*filters.waste_types[0] == '\0') {
            waste_type_match = true;
        } else {
            for (size_t j = 0; j < filters.waste_type_count; j++) {
                char waste_type = filters.waste_types[j][0]; // Access the first character of the waste type string
                switch (waste_type) {
                    case 'A':
                        waste_type_match |= (strcmp(type, "Plastics and Aluminium") == 0);
                        break;
                    case 'P':
                        waste_type_match |= (strcmp(type, "Paper") == 0);
                        break;
                    case 'B':
                        waste_type_match |= (strcmp(type, "Biodegradable waste") == 0);
                        break;
                    case 'G':
                        waste_type_match |= (strcmp(type, "Clear glass") == 0);
                        break;
                    case 'C':
                        waste_type_match |= (strcmp(type, "Colored glass") == 0);
                        break;
                    case 'T':
                        waste_type_match |= (strcmp(type, "Textile") == 0);
                        break;
                }
                if (waste_type_match) {
                    break;
                }
            }
        }

        bool capacity_match = ((filters.capacity_min == 0 && filters.capacity_max == 0) ||
                               (capacity >= filters.capacity_min && capacity <= filters.capacity_max));

        bool public_match = (filters.public_filter == -1 || public_value == filters.public_filter);

        if (waste_type_match && capacity_match && public_match) {
            printf("ID: ");
            printf("%s",data_source->containers[i][0]); // ID
            printf(", ");
            printf("Type: ");
            printf("%s",data_source->containers[i][3]); // Type
            printf(", ");
            printf("Capacity: ");
            printf("%s",data_source->containers[i][4]); // Container Capacity
            printf(", ");
            printf("Address: ");
            printf("%s %s",data_source->containers[i][6], data_source->containers[i][7]); // Street
            printf(", ");
            printf("Neighbors: ");
            size_t neighbors_count;
            Neighbor *neighbors = find_neighbors(data_source->containers[i][0], &neighbors_count);
            for (size_t j = 0; j < neighbors_count; j++) {
                printf("%s", neighbors[j].id);
                if (j < neighbors_count - 1) {
                    printf(" ");
                }
            }
            free(neighbors);
            printf("\n");
        }
    }
}

int compare_size_t(const void *a, const void *b) {
    size_t arg1 = *(const size_t *) a;
    size_t arg2 = *(const size_t *) b;
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}
char get_waste_type_char(const char *type){
    if(strcmp(type, "Plastics and Aluminium")==0)  return 'A';
    else if(strcmp(type, "Paper")==0) return 'P';
    else if(strcmp(type, "Biodegradable waste") == 0) return 'B';
    else if(strcmp(type, "Clear glass") == 0) return 'G';
    else if(strcmp(type, "Colored glass") == 0) return 'C';
    else if(strcmp(type, "Textile") == 0) return 'T';
    else return '\0';
}
char* set_order(char* waste_types){
    size_t len = strlen(waste_types);
    char* new_waste_types = malloc((len + 1) * sizeof(char));

    int index=0;
    if (strchr(waste_types, 'A')) {
        new_waste_types[index++]='A';
    }
    if (strchr(waste_types, 'P')) {
        new_waste_types[index++]='P';
    }
    if (strchr(waste_types, 'B')) {
        new_waste_types[index++]='B';
    }
    if (strchr(waste_types, 'G')) {
        new_waste_types[index++]='G';
    }
    if (strchr(waste_types, 'C')) {
        new_waste_types[index++]='C';
    }
    if (strchr(waste_types, 'T')) {
        new_waste_types[index++]='T';
    }
    new_waste_types[index++] = '\0';
    return new_waste_types;            
}
/// @brief 
/// @param  
void print_stations(void) {

    Station *stations = NULL;
    size_t stations_count = 0;
    
    for (size_t i = 0; i < data_source->containers_count; i++) {
        
        const char *container_id = get_container_id(i);
        const char *waste_type = get_container_waste_type(i);
        const char waste_type_char = get_waste_type_char(waste_type);
        double container_x = strtod(get_container_x(i), NULL);
        double container_y = strtod(get_container_y(i), NULL);
    
        bool found_existing_station = false;
        for (size_t j = 0; j < stations_count && !found_existing_station; j++) {
            
            double station_container_x = stations[j].x; 
            double station_container_y = stations[j].y;

            double x_diff = station_container_x - container_x;
            double y_diff = station_container_y - container_y;
            double distance = sqrt(x_diff * x_diff + y_diff * y_diff);
            const double tolerance= 1e-14;

            if (distance < tolerance) {
                found_existing_station = true;

                stations[j].containerIds = (size_t*)realloc(stations[j].containerIds,
                                (stations[j].containers_count + 1) * sizeof(size_t));
                stations[j].containerIds[stations[j].containers_count] = atoi(container_id);
                stations[j].containers_count++;
                
                // Add waste_type to station waste_types if it's not already present
                if (!strchr(stations[j].waste_types, waste_type_char)) {
                    size_t len = strlen(stations[j].waste_types);
                    stations[j].waste_types = realloc(stations[j].waste_types, len + 2);
                    stations[j].waste_types[len] = waste_type_char;
                    stations[j].waste_types[len + 1] = '\0';
                }

                // Add neighbors to the existing station
                size_t neighbors_count;
                Neighbor *neighbors = find_neighbors(container_id, &neighbors_count);
                for (size_t k = 0; k < neighbors_count; k++) {
                    const char *neighbor_id = neighbors[k].id;
                    size_t neighbor_station_id = 0;
                    
                    for (size_t l = 0; l < stations_count; l++) {
                        for (size_t m = 0; m < stations[l].containers_count; m++) {
                    
                            if (stations[l].containerIds[m]== strtoul(neighbor_id, NULL, 10)) {
                                neighbor_station_id = stations[l].id;
                                break;
                            }
                        }

                        if (neighbor_station_id > 0) {
                            bool already_added = false;
                            for (size_t l = 0; l < stations[j].neighbors_count; l++) {
                                if (stations[j].neighbors[l] == neighbor_station_id) {
                                    already_added = true;
                                    break;
                                }
                            }
                            if (!already_added) {
                                stations[j].neighbors = realloc(stations[j].neighbors,
                                                                (stations[j].neighbors_count + 1) * sizeof(size_t));
                                stations[j].neighbors[stations[j].neighbors_count] = neighbor_station_id;
                                stations[j].neighbors_count++;
                            }
                        }
                    }
                    
                }
                free(neighbors);
            }
        }
        if (!found_existing_station) {
            // Create a new station
            stations_count++;
            stations = realloc(stations, stations_count * sizeof(Station));
            
            Station *new_station = &stations[stations_count - 1];
            new_station->containers_count=0;
            new_station->containerIds = (size_t*)realloc(NULL,
                                (new_station->containers_count + 1) * sizeof(size_t));
            new_station->containerIds[new_station->containers_count] = atoi(container_id);
            new_station->containers_count++;
            new_station->id = stations_count;
            new_station->x = container_x;
            new_station->y = container_y;
            new_station->waste_types = malloc(2 * sizeof(char));
            new_station->waste_types[0] = waste_type_char;
            new_station->waste_types[1] = '\0';
            new_station->neighbors_count = 0;
            new_station->neighbors = NULL;
            new_station->cid = container_id;


            // Add neighbors to the new station
            size_t neighbors_count;
            Neighbor *neighbors = find_neighbors(container_id, &neighbors_count);
            for (size_t k = 0; k < neighbors_count; k++) {
                const char *neighbor_id = neighbors[k].id;
                size_t neighbor_station_id = 0;
                for (size_t l = 0; l < stations_count - 1; l++) {
                    for (size_t m = 0; m < stations[l].containers_count; m++) {
                
                        if (stations[l].containerIds[m]== strtoul(neighbor_id, NULL, 10)) {
                            neighbor_station_id = stations[l].id;
                            break;
                        }
                    }
                    
                if (neighbor_station_id > 0) {
                    new_station->neighbors = realloc(new_station->neighbors,
                                                     (new_station->neighbors_count + 1) * sizeof(size_t));
                    new_station->neighbors[new_station->neighbors_count] = neighbor_station_id;
                    new_station->neighbors_count++;

                    // Add the new station as a neighbor to the existing station
                    Station *neighbor_station = &stations[neighbor_station_id - 1];
                    neighbor_station->neighbors = realloc(neighbor_station->neighbors,
                                                          (neighbor_station->neighbors_count + 1) * sizeof(size_t));
                    neighbor_station->neighbors[neighbor_station->neighbors_count] = new_station->id;
                    neighbor_station->neighbors_count++;
                }
                }
            }
            free(neighbors);
        }
    }

    // Print stations
    for (size_t i = 0; i < stations_count; i++) {
        printf("%zu;%s;", stations[i].id, set_order(stations[i].waste_types));
        
        // Sort the neighbors array
        qsort(stations[i].neighbors, stations[i].neighbors_count, sizeof(size_t), compare_size_t);
        
        // Create a temporary array to store the unique neighbors and initialize unique_count
        size_t *unique_neighbors = malloc(stations[i].neighbors_count * sizeof(size_t));
        size_t unique_count = 0;
        
        // Iterate through the neighbors array and only add unique elements to the unique_neighbors array
        for (size_t j = 0; j < stations[i].neighbors_count; j++) {
            if (j == 0 || stations[i].neighbors[j] != stations[i].neighbors[j - 1]) {
                unique_neighbors[unique_count++] = stations[i].neighbors[j];
            }
        }
        
        // Free the old neighbors array and update the station's neighbors array and count
        free(stations[i].neighbors);
        stations[i].neighbors = unique_neighbors;
        stations[i].neighbors_count = unique_count;

        // Print the neighbors
        for (size_t j = 0; j < stations[i].neighbors_count; j++) {
            printf("%zu", stations[i].neighbors[j]);
            if (j < stations[i].neighbors_count - 1) {
                printf(",");
            }
        }
        printf("\n");
    }
    
    // Free memory
    for (size_t i = 0; i < stations_count; i++) {
        free(stations[i].containerIds);
        free(stations[i].waste_types);
        free(stations[i].neighbors);
    }
    free(stations);
    
}

