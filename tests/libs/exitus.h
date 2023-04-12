#ifndef EXITUS_H
#define EXITUS_H

int exitus_count_args(char **argv);
int exitus(int (*m)(int, char**), int argc, char **argv);

#endif // EXITUS_H
