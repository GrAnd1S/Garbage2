#ifndef DATA_SOURCE_H
#define DATA_SOURCE_H

#include <stdbool.h>
#include <stdlib.h>

/**
 * @brief Initializes internal data storage.
 * 
 * Reads the input files and prepares internal data storage
 * to be used by following get_* functions. It is necessary to call this
 * function before any of the following. Otherwise, their behavior is undefined
 * and probably ends with SIGSEGV.
 * 
 * @note The only validation of input files within this function is
 * the counting of columns of the input CSV files. It basically counts ','
 * in each line. Any other validation of the data is up to you ;)
 * 
 * @warning This function allocates memory. To avoid memory leaks is necessary
 * to call destroy_data_source() before ending the program.
 * 
 * @param containers_path Path to the CSV file with containers, e.g., Brno-JundrovContainers.csv.
 * 
 * @param paths_path Path to the CSV file with paths between containers, e.g., Brno-JundrovPaths.csv.
 * 
 * @retval true if no error occurs.
 * 
 * @retval false in case of error, i.e., memory failure, inaccessible input file
 * or unexpected count of columns.
 */
bool init_data_source(const char *containers_path, const char *paths_path);

/**
 * @brief Frees all memory allocated by the data source.
 * 
 * If you don't call this function before ending the program, Valgrind will haunt you in your dreams.
 * 
 * @warning Using this function before initialization of the data source has undefined behavior.
 * Using get_* functions after calling this function also has undefined behavior.
 */
void destroy_data_source(void);

/**
 * @brief Selects the container ID from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the first column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_id(size_t line_index);

/**
 * @brief Selects the container's X coordinate from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the second column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_x(size_t line_index);

/**
 * @brief Selects the container's Y coordinate from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the third column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_y(size_t line_index);

/**
 * @brief Selects the container waste type from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the fourth column of the selected line.
 * @retval NULL if the line_index is out of range
 */
const char *get_container_waste_type(size_t line_index);

/**
 * @brief Selects the container capacity from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the fifth column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_capacity(size_t line_index);

/**
 * @brief Selects the container name from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the sixth column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_name(size_t line_index);

/**
 * @brief Selects the container street name from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the seventh column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_street(size_t line_index);

/**
 * @brief Selects the container house number from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the eighth column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_number(size_t line_index);

/**
 * @brief Selects the container accessibility from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the ninth column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_container_public(size_t line_index);

/**
 * @brief Selects the ID of the first container in the path from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the first column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_path_a_id(size_t line_index);

/**
 * @brief Selects the ID of the second container in the path from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the second column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_path_b_id(size_t line_index);

/**
 * @brief Selects the path length from the currently loaded CSV in data storage.
 * 
 * @warning Changing content of the returned pointer is highly discouraged.
 * 
 * @param line_index Number of the wanted line (starts from 0).
 * @retval char* pointer to content of the third column of the selected line.
 * @retval NULL if the line_index is out of range.
 */
const char *get_path_distance(size_t line_index);

typedef struct {
    char waste_types[8][2];
    size_t waste_type_count;
    int capacity_min;
    int capacity_max;
    int public_filter;
    const char *containers_path;
    const char *paths_path;
    int special_flag;
} Filters;


// Update the function prototype
void print_containers(Filters filters);
void print_locations(void);
void print_stations(void);
char get_waste_type_char(const char *type);
char* set_order(char* waste_types);
#endif // DATA_SOURCE_H
