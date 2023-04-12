#ifndef CONTAINER_H
#define CONTAINER_H

#include <stdbool.h>

typedef struct Container {
    char *id;
    double x;   // Latitude
    double y;   // Longitude
    char *waste_type;
    double capacity;
    char *name;
    char *street;
    char *number;
    bool is_public;
} Container;

// Allocates memory and creates a new Container with the provided values.
Container *create_container(const char *id, float x, float y, const char *waste_type, float capacity,
                            const char *name, const char *street, const char *number, bool is_public);

// Frees the memory allocated for a Container and its attributes.
void destroy_container(Container *container);

// Prints the information of a Container.
void print_container(const Container *container);

#endif // CONTAINER_H
