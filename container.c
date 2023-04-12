#define _DEFAULT_SOURCE
#include "container.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

Container *create_container(const char *id, float x, float y, const char *waste_type, float capacity, const char *name,
                            const char *street, const char *number, bool is_public) {
    Container *container = (Container *) malloc(sizeof(Container));

    if (container == NULL) {
        return NULL;
    }

    container->id = strdup(id);
    container->x = x;
    container->y = y;
    container->waste_type = strdup(waste_type);
    container->capacity = capacity;
    container->name = name ? strdup(name) : NULL;
    container->street = street ? strdup(street) : NULL;
    container->number = number ? strdup(number) : NULL;
    container->is_public = is_public;

    return container;
}

void destroy_container(Container *container) {
    if (container != NULL) {
        free(container->id);
        free(container->waste_type);
        if (container->name != NULL) {
            free(container->name);
        }
        if (container->street != NULL) {
            free(container->street);
        }
        if (container->number != NULL) {
            free(container->number);
        }
        free(container);
    }
}

void print_container(const Container *container) {
    printf("ID: %s\n", container->id);
    printf("X: %.2f\n", container->x);
    printf("Y: %.2f\n", container->y);
    printf("Waste Type: %s\n", container->waste_type);
    printf("Capacity: %.2f\n", container->capacity);

    if (container->name != NULL) {
        printf("Name: %s\n", container->name);
    } else {
        printf("Name: Not available\n");
    }

    if (container->street != NULL) {
        printf("Street: %s\n", container->street);
    } else {
        printf("Street: Not available\n");
    }

    if (container->number != NULL) {
        printf("Number: %s\n", container->number);
    } else {
        printf("Number: Not available\n");
    }

    printf("Is Public: %s\n", container->is_public ? "Yes" : "No");
}
