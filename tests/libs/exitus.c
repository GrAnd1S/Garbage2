/* We want to use non-standard ‹on_exit()› function to be able to intercept
 * explicit ‹exit()› calls. This feature macro will unhide its definition in
 * ‹stdlib.h›. Be aware though that Kontr will check «your»
 * files for such macros and will throw a tantrum if it finds any. */
#define _GNU_SOURCE

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Do NOT use ‹setjmp()› in your code, especially not in your homework.
 * It is like ‹goto›, except on steroids, as it can jump between functions
 * on the stack. This is «NOT» what you usually want to do and here it is
 * only used to intercept the ‹exit()› call, which cannot be done in any
 * other way.
 *
 * IF YOU USE ‹setjmp()› IN YOUR HOMEWORK, YOU WILL HAVE TO REWRITE IT
 * ANYWAY. DON'T EVEN TRY THAT.
 *
 * The implementation here also relies on some GNU stuff and, frankly,
 * on some undefined behaviour (it is undefined what happens when
 * ‹longjmp()› is called from a function called when destructors are called). */
#include <setjmp.h>

struct exitus {
    /* This buffer will hold a reference to stackframe of a function
     * that will intercept the ‹exit()› call. */
    jmp_buf handler;
    /* However, we only want to intercept ‹exit()› calls from inside
     * the context of ‹exitus()›. If this flag is off, the intercepting
     * function will simply do nothing. */
    bool armed;
};

/* Even though we get a pointer to this structure in ‹on_exit()› handler,
 * we cannot control «where» the handler gets called. It is possible that
 * the handler would run in a context where the local variable would already
 * be destroyed, leading into (another) undefined behaviour. */
static
struct exitus _exitus;

static
void _exitus_avoid(int code, void *context)
{
    struct exitus *exitus = context;

    if (!exitus->armed)
        return;

    exitus->armed = false;
    /* ‹longjmp()› here will jump back to the call ‹setjmp()›, but it always
     * returns a non-zero code in such a case (even when we pass 0 to the
     * second argument. For this reason we artificially add ‹1› to the higher
     * byte of the return code.
     *
     * Also note that calling ‹longjmp()› from an exit handler causes undefined
     * behaviour. It happens to work on Aisa, but… */
    longjmp(exitus->handler, (1 << 8) | (code & 0xff));
}

int exitus_count_args(char **argv)
{
    int counter = 0;

    for (char **p = argv; *p != NULL; ++p)
        ++counter;

    return counter;
}

int exitus(int (*m)(int, char**), int argc, char **argv)
{
    assert(m != NULL);
    assert(argv != NULL);

    /* Just a sanity check. */
    assert(exitus_count_args(argv) == argc);

    memset(&_exitus, 0, sizeof(_exitus));

    int code;
    if ((code = setjmp(_exitus.handler)) != 0) {
        /* The returned code will have ‹1› in the higher byte to make sure
         * exit code ‹0› doesn't get mangled. So, here we decode it. */
        return code & 0xff;
    }

    if (on_exit(&_exitus_avoid, &_exitus) != 0) {
        fprintf(stderr, "main_exit_intercept: Cannot set up on_exit handler\n");
        abort();
    }

    _exitus.armed = true;
    int retval = m(exitus_count_args(argv), argv);
    _exitus.armed = false;
    return retval;
}
